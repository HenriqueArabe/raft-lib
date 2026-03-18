package raft

import (
	"fmt"
	"math"

	log "github.com/sirupsen/logrus"

	"github.com/henrique-arab/raft-lib/types"
)

// ---- nodeState log methods -----------------------------------------

// addCommandLogLocked appends a new LogCommand entry to the leader's log.
// Triggers a snapshot if the array is full.
// mu must NOT be held; the method acquires it internally.
// Returns the new log index on success, or -1 on failure.
func (s *nodeState) addCommandLog(id types.ServerID, data []byte) int {
	s.mu.Lock()
	defer s.mu.Unlock()

	lastIdx, _ := s.getLastLogIdxTerm()
	entry := types.RaftLog{
		Index: lastIdx + 1,
		Term:  s.currentTerm,
		Type:  types.LogCommand,
		Data:  data,
	}

	if s.nextLogIdx >= logArrayCapacity {
		// TODO: Session 4 — call takeSnapshot, check if space freed.
		return -1
	}

	s.logs[s.nextLogIdx] = entry
	s.logHashes[s.nextLogIdx] = s.getLogHash(entry)
	s.nextLogIdx++

	log.Infof("State - add command log: idx=%d term=%d hash=%x",
		entry.Index, entry.Term, s.logHashes[s.nextLogIdx-1])

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

	log.Infof("State - add noop log: idx=%d term=%d", entry.Index, entry.Term)
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
		log.Tracef("Sending %d logs to %s (start=%d end=%d)",
			len(logsToSend), id, logsToSend[0].Index, logsToSend[len(logsToSend)-1].Index)
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
// TODO: Session 3 — full implementation.
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
		log.Info("Become Follower (handleAppendEntries)")
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
		// TODO: Session 4 — attempt snapshot then purge.
		return &types.AppendEntriesResponse{ID: myID, Term: s.currentTerm, Success: false, LastIndex: lastIdx}
	}

	for i, entry := range aea.Entries {
		pos := startNext + i
		if pos >= s.nextLogIdx {
			s.logs[pos] = entry
			s.nextLogIdx = pos + 1
			s.logHashes[pos] = s.getLogHash(entry)
			log.Infof("State - add raft log: idx=%d term=%d hash=%x",
				entry.Index, entry.Term, fmt.Sprintf("%x", s.logHashes[pos]))
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
			log.Info("Become Follower (handleAppendEntriesResponse)")
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
	log.Tracef("State: updateLastApplied log=%d arr=%d commit=%d",
		s.lastApplied, arrIdx, s.commitIndex)
	return arrIdx
}

// getLog returns the log entry at array position i.
func (s *nodeState) getLog(i int) types.RaftLog {
	return s.logs[i]
}

// ---- RaftNode log helpers ------------------------------------------

// sendAppendEntriesRPCs sends AppendEntries to all current voting peers.
// Also sends to unvoting (joining) peers so they catch up.
// TODO: Session 3 — implement connection iteration.
func (n *RaftNode) sendAppendEntriesRPCs() {
	// TODO: Session 3 — iterate n.connections and n.unvotingConnections,
	// call n.state.getAppendEntriesArgs(id) and n.transport.SendAppendEntries.
	// If getAppendEntriesArgs returns nil, call sendInstallSnapshotRPC instead.
}

// handleAppendEntriesRPCResponses is the goroutine that drains myAEResponseChan.
// TODO: Session 3 — implement.
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
			_ = matchIdx
			// TODO: Session 3 — check unvoting promotion.
		case <-n.stopCh:
			return
		}
	}
}

// applyLog sends a committed log entry to the state machine.
func (n *RaftNode) applyLog(entry types.RaftLog) {
	log.Infof("Raft apply log: idx=%d type=%d", entry.Index, entry.Type)
	if entry.Type == types.LogCommand {
		if err := n.sm.Apply(entry.Data); err != nil {
			log.Errorf("StateMachine.Apply error: %v", err)
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
		// For configuration entries, unlock the config pipeline.
		if entry.Type == types.LogConfiguration {
			n.state.unlockNextConfiguration()
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
