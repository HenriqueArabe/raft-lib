package raft

import (
	"fmt"
	"log/slog"
	"math"
	"time"

	"github.com/henrique-arab/raft-lib/types"
)

// ---- nodeState log methods -----------------------------------------

// addCommandLogLocked appends a new LogCommand entry to the leader's log.
// Triggers a snapshot if the array is full.
// mu must NOT be held; the method acquires it internally.
// Returns the new log index on success, or -1 on failure.
func (s *nodeState) addCommandLog(id types.ServerID, clientID string, seqNum int64, data []byte) int {
	s.mu.Lock()
	defer s.mu.Unlock()

	lastIdx, _ := s.getLastLogIdxTerm()
	entry := types.RaftLog{
		Index:    lastIdx + 1,
		Term:     s.currentTerm,
		Type:     types.LogCommand,
		Data:     data,
		ClientID: clientID,
		SeqNum:   seqNum,
	}

	if s.nextLogIdx >= logArrayCapacity {
		if !s.takeSnapshot() || s.nextLogIdx >= logArrayCapacity {
			return -1
		}
	}

	s.logs[s.nextLogIdx] = entry
	s.logHashes[s.nextLogIdx] = s.getLogHash(entry)
	s.nextLogIdx++

	slog.Info(fmt.Sprintf("State - add command log: idx=%d term=%d hash=%x",
		entry.Index, entry.Term, s.logHashes[s.nextLogIdx-1]))

	s.matchIndex[id] = entry.Index
	s.lastSentLogIndex[id] = entry.Index
	s.nextIndex[id] = entry.Index + 1

	return entry.Index
}

// addNoopLogLocked appends a no-op entry. mu must be held.
func (s *nodeState) addNoopLogLocked(id types.ServerID) {
	lastIdx, _ := s.getLastLogIdxTerm()
	entry := types.RaftLog{
		Index: lastIdx + 1,
		Term:  s.currentTerm,
		Type:  types.LogNoop,
	}
	if s.nextLogIdx >= logArrayCapacity {
		return
	}
	s.logs[s.nextLogIdx] = entry
	s.logHashes[s.nextLogIdx] = s.getLogHash(entry)
	s.nextLogIdx++

	slog.Info(fmt.Sprintf("State - add noop log: idx=%d term=%d", entry.Index, entry.Term))
	s.matchIndex[id] = entry.Index
	s.lastSentLogIndex[id] = entry.Index
	s.nextIndex[id] = entry.Index + 1
}

// getAppendEntriesArgs builds AppendEntries args for the given follower.
// Returns nil if the next entry is inside a snapshot (use InstallSnapshot instead).
func (s *nodeState) getAppendEntriesArgs(id types.ServerID) *types.AppendEntriesArgs {
	s.mu.Lock()
	defer s.mu.Unlock()

	myLastIdx, myLastTerm := s.getLastLogIdxTerm()
	serverNextIdx := s.nextIndex[id]
	found, arrLogIdx := s.findArrayIdxByLogIndex(serverNextIdx)

	if !found && serverNextIdx <= myLastIdx {
		// Entry is inside a snapshot — caller should send InstallSnapshot.
		return nil
	}

	var logsToSend []types.RaftLog
	if arrLogIdx >= 0 && arrLogIdx < s.nextLogIdx {
		logsToSend = make([]types.RaftLog, s.nextLogIdx-arrLogIdx)
		copy(logsToSend, s.logs[arrLogIdx:s.nextLogIdx])
	}

	// Determine prevLogIndex / prevLogTerm for the receiver's consistency check.
	var prevLogIdx, prevLogTerm int
	switch {
	case arrLogIdx < 0:
		prevLogIdx = myLastIdx
		prevLogTerm = myLastTerm
	case arrLogIdx == 0 && s.lastSnapshot != nil:
		prevLogIdx = s.lastSnapshot.LastIncludedIndex
		prevLogTerm = s.lastSnapshot.LastIncludedTerm
	case arrLogIdx > 0:
		prev := s.logs[arrLogIdx-1]
		prevLogIdx = prev.Index
		prevLogTerm = prev.Term
	default:
		prevLogIdx = -1
		prevLogTerm = -1
	}

	if len(logsToSend) > 0 {
		s.lastSentLogIndex[id] = logsToSend[len(logsToSend)-1].Index
		slog.Debug(fmt.Sprintf("Sending %d logs to %s (start=%d end=%d)",
			len(logsToSend), id, logsToSend[0].Index, logsToSend[len(logsToSend)-1].Index))
	} else {
		s.lastSentLogIndex[id] = serverNextIdx - 1
	}

	return &types.AppendEntriesArgs{
		Term:         s.currentTerm,
		LeaderID:     s.currentLeader,
		PrevLogIndex: prevLogIdx,
		PrevLogTerm:  prevLogTerm,
		Entries:      logsToSend,
		LeaderCommit: s.commitIndex,
	}
}

// handleAppendEntries processes an incoming AppendEntries RPC (follower side).
func (s *nodeState) handleAppendEntries(myID types.ServerID, aea *types.AppendEntriesArgs) *types.AppendEntriesResponse {
	s.mu.Lock()
	defer s.mu.Unlock()

	lastIdx, _ := s.getLastLogIdxTerm()

	// 1. Reject stale term.
	if aea.Term < s.currentTerm {
		return &types.AppendEntriesResponse{ID: myID, Term: s.currentTerm, Success: false, LastIndex: lastIdx}
	}

	// Advance term if necessary.
	if aea.Term > s.currentTerm {
		s.updateCurrentTerm(aea.Term)
	}

	// Step down from Candidate or Leader.
	if s.role == Candidate || s.role == Leader {
		s.stopElectionTimeoutLocked()
		slog.Info("Become Follower (handleAppendEntries)")
		s.role = Follower
	}

	// 2. Check prevLog consistency.
	var prevLogArrIdx = -1
	if aea.PrevLogIndex >= 0 {
		found, arrIdx := s.findArrayIdxByLogIndex(aea.PrevLogIndex)
		if !found {
			if s.lastSnapshot == nil || s.lastSnapshot.LastIncludedIndex < aea.PrevLogIndex {
				return &types.AppendEntriesResponse{ID: myID, Term: s.currentTerm, Success: false, LastIndex: lastIdx}
			}
		} else {
			if s.logs[arrIdx].Term != aea.PrevLogTerm {
				return &types.AppendEntriesResponse{ID: myID, Term: s.currentTerm, Success: false, LastIndex: lastIdx}
			}
			prevLogArrIdx = arrIdx
		}
	}

	// Heartbeat: update leader and return success.
	s.currentLeader = aea.LeaderID

	// 3 & 4. Merge incoming entries.
	startNext := prevLogArrIdx + 1
	if startNext+len(aea.Entries) >= logArrayCapacity {
		if !s.takeSnapshot() || startNext+len(aea.Entries) >= logArrayCapacity {
			return &types.AppendEntriesResponse{ID: myID, Term: s.currentTerm, Success: false, LastIndex: lastIdx}
		}
	}

	for i, entry := range aea.Entries {
		pos := startNext + i
		if pos >= s.nextLogIdx {
			s.logs[pos] = entry
			s.nextLogIdx = pos + 1
			s.logHashes[pos] = s.getLogHash(entry)
			slog.Info(fmt.Sprintf("State - add raft log: idx=%d term=%d hash=%x",
				entry.Index, entry.Term, s.logHashes[pos]))
			if entry.Type == types.LogConfiguration {
				s.applyConfigurationLocked(pos, entry.Config)
			}
		} else {
			if s.logs[pos].Index != entry.Index || s.logs[pos].Term != entry.Term {
				// Conflict: truncate and replace.
				if s.logs[pos].Index <= s.lastConfigLogIndex {
					// Revert configuration.
					s.serverConfig = copyConfig(s.oldServerConfig)
				}
				s.logs[pos] = entry
				s.nextLogIdx = pos + 1
				s.logHashes[pos] = s.getLogHash(entry)
				if entry.Type == types.LogConfiguration {
					s.applyConfigurationLocked(pos, entry.Config)
				}
			}
		}
	}

	// 5. Advance commit index.
	if aea.LeaderCommit > s.commitIndex {
		if len(aea.Entries) > 0 {
			lastEntry := aea.Entries[len(aea.Entries)-1]
			s.commitIndex = int(math.Min(float64(aea.LeaderCommit), float64(lastEntry.Index)))
		} else {
			s.commitIndex = aea.LeaderCommit
		}
	}

	return &types.AppendEntriesResponse{ID: myID, Term: s.currentTerm, Success: true, LastIndex: lastIdx}
}

// handleAppendEntriesResponse processes a reply from a follower.
// Returns (matchIndex, needSnapshot).
func (s *nodeState) handleAppendEntriesResponse(myID types.ServerID, aer *types.AppendEntriesResponse) (int, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !aer.Success {
		if aer.Term > s.currentTerm {
			slog.Info("Become Follower (handleAppendEntriesResponse)")
			s.role = Follower
			s.updateCurrentTerm(aer.Term)
			s.currentLeader = aer.ID
			return -1, false
		}
		// Roll back nextIndex for this follower.
		if aer.LastIndex >= 0 {
			s.nextIndex[aer.ID] = aer.LastIndex
		} else {
			s.nextIndex[aer.ID] = 0
		}
		needSnapshot := s.lastSnapshot != nil &&
			s.nextIndex[aer.ID] <= s.lastSnapshot.LastIncludedIndex
		return -1, needSnapshot
	}

	s.nextIndex[aer.ID] = s.lastSentLogIndex[aer.ID] + 1
	s.matchIndex[aer.ID] = s.lastSentLogIndex[aer.ID]
	return s.matchIndex[aer.ID], false
}

// checkCommits advances commitIndex if a majority have replicated a newer entry.
// Must only be called on the Leader.
func (s *nodeState) checkCommits() {
	s.mu.Lock()
	defer s.mu.Unlock()

	for i := s.nextLogIdx - 1; i >= 0; i-- {
		if s.logs[i].Index <= s.commitIndex {
			break
		}
		// Count how many nodes have replicated this entry.
		replicated := 0
		for sid := range s.matchIndex {
			if s.matchIndex[sid] >= s.logs[i].Index {
				replicated++
			}
		}
		if replicated > len(s.serverConfig)/2 && s.logs[i].Term == s.currentTerm {
			s.commitIndex = s.logs[i].Index
			break
		}
	}
}

// updateLastApplied advances lastApplied by one step if possible.
// Returns the array index of the entry to apply, or -1 if nothing to apply.
func (s *nodeState) updateLastApplied() int {
	if s.commitIndex <= s.lastApplied {
		return -1
	}
	_, arrIdx := s.findArrayIdxByLogIndex(s.lastApplied + 1)
	if arrIdx >= 0 {
		s.lastApplied++
	}
	slog.Debug(fmt.Sprintf("State: updateLastApplied log=%d arr=%d commit=%d",
		s.lastApplied, arrIdx, s.commitIndex))
	return arrIdx
}

// getLog returns the log entry at array position i.
func (s *nodeState) getLog(i int) types.RaftLog {
	return s.logs[i]
}

// ---- RaftNode log helpers ------------------------------------------

// sendAppendEntriesRPCs sends AppendEntries to all current voting peers.
// Also sends to unvoting (joining) peers so they catch up.
func (n *RaftNode) sendAppendEntriesRPCs() {
	const aeTimeout = 200 * time.Millisecond

	send := func(peer types.ServerID) {
		args := n.state.getAppendEntriesArgs(peer)
		if args == nil {
			// Peer is behind the last snapshot; InstallSnapshot handles it (Session 4).
			n.sendInstallSnapshotRPC(peer)
			return
		}
		go func() {
			resp, err := n.transport.SendAppendEntries(peer, args)
			if err != nil {
				slog.Debug(fmt.Sprintf("AppendEntries to %s: %v", peer, err))
				return
			}
			select {
			case n.myAEResponseChan <- resp:
			case <-time.After(aeTimeout):
				slog.Warn(fmt.Sprintf("AppendEntries response from %s not consumed (timeout)", peer))
			case <-n.stopCh:
			}
		}()
	}

	for _, peer := range n.cfg.Peers {
		send(peer)
	}
	for _, peer := range n.state.getUnvotingServerIDs() {
		send(peer)
	}
}

// handleAppendEntriesRPCResponses is the goroutine that drains myAEResponseChan.
func (n *RaftNode) handleAppendEntriesRPCResponses() {
	for {
		select {
		case resp := <-n.myAEResponseChan:
			if n.state.getRole() != Leader {
				continue
			}
			matchIdx, needSnapshot := n.state.handleAppendEntriesResponse(n.cfg.ID, resp)
			if needSnapshot {
				n.sendInstallSnapshotRPC(resp.ID)
			}
			// If an unvoting server has caught up to the commit index, promote it.
			if matchIdx >= 0 && matchIdx >= n.state.getCommitIndex() {
				n.promoteUnvotingServer(resp.ID) // no-op if not an unvoting server
			}
		case <-n.stopCh:
			return
		}
	}
}

// applyLog sends a committed log entry to the state machine.
func (n *RaftNode) applyLog(entry types.RaftLog) {
	slog.Info(fmt.Sprintf("Raft apply log: idx=%d type=%d", entry.Index, entry.Type))
	n.metrics.recordCommit()
	if entry.Type == types.LogCommand {
		if err := n.sm.Apply(entry.Data); err != nil {
			slog.Error(fmt.Sprintf("StateMachine.Apply error: %v", err))
		}
		// Update deduplication tracking so replayed requests are detected.
		if entry.ClientID != "" && entry.SeqNum > 0 {
			n.state.setClientLastSeq(types.ServerID(entry.ClientID), entry.SeqNum)
		}
	}
	// Signal pending command if we are the leader.
	if n.state.getRole() == Leader {
		n.pendingMu.Lock()
		if ch, ok := n.pendingCommands[entry.Index]; ok {
			delete(n.pendingCommands, entry.Index)
			n.pendingMu.Unlock()
			ch <- nil
		} else {
			n.pendingMu.Unlock()
		}
		// For configuration entries, unlock the config pipeline and
		// signal the caller waiting in waitForConfigApplied.
		if entry.Type == types.LogConfiguration {
			n.state.unlockNextConfiguration()
			n.pendingConfigMu.Lock()
			if ch, ok := n.pendingConfigApplied[entry.Index]; ok {
				delete(n.pendingConfigApplied, entry.Index)
				n.pendingConfigMu.Unlock()
				ch <- true
			} else {
				n.pendingConfigMu.Unlock()
			}
		}
	}
}

// checkLogsToApply applies all committed-but-not-yet-applied entries.
func (n *RaftNode) checkLogsToApply() {
	for {
		arrIdx := n.state.updateLastApplied()
		if arrIdx < 0 {
			return
		}
		n.applyLog(n.state.getLog(arrIdx))
	}
}
