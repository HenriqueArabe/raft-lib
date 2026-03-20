package raft

import (
	log "github.com/sirupsen/logrus"

	"github.com/henrique-arab/raft-lib/types"
)

// ---- nodeState snapshot methods ------------------------------------

// takeSnapshot captures the state machine state and compacts the log.
// It signals n.snapshotRequestChan and waits for the state machine snapshot
// on n.snapshotResponseChan (the RaftNode bridges those to the StateMachine).
// Returns true if the snapshot succeeded.
// Must be called with s.mu held. snapshotLoop does not acquire s.mu,
// so the channel round-trip is safe while the lock is held.
func (s *nodeState) takeSnapshot() bool {
	// Ask the RaftNode to take a snapshot of the application state machine.
	s.snapshotRequestChan <- struct{}{}
	gameState := <-s.snapshotResponseChan

	found, lastAppliedArrIdx := s.findArrayIdxByLogIndex(s.lastApplied)
	if !found {
		return false
	}

	log.Tracef("Taking snapshot: arr=%d idx=%d term=%d",
		lastAppliedArrIdx,
		s.logs[lastAppliedArrIdx].Index,
		s.logs[lastAppliedArrIdx].Term)

	snap := &types.Snapshot{
		LastIncludedIndex: s.logs[lastAppliedArrIdx].Index,
		LastIncludedTerm:  s.logs[lastAppliedArrIdx].Term,
		Data:              gameState,
		ServerConfig:      copyConfig(s.serverConfig),
		Hash:              s.logHashes[lastAppliedArrIdx],
	}
	s.lastSnapshot = snap

	// Compact: keep only entries after the snapshot point.
	s.copyLogsToBeginning(lastAppliedArrIdx + 1)
	return true
}

// prepareInstallSnapshotRPC builds InstallSnapshot args from the last snapshot.
func (s *nodeState) prepareInstallSnapshotRPC(leaderID types.ServerID) *types.InstallSnapshotArgs {
	s.mu.Lock()
	defer s.mu.Unlock()

	snap := s.lastSnapshot
	return &types.InstallSnapshotArgs{
		ID:                  leaderID,
		Term:                s.currentTerm,
		LastIncludedIndex:   snap.LastIncludedIndex,
		LastIncludedTerm:    snap.LastIncludedTerm,
		Data:                snap.Data,
		ServerConfiguration: copyConfig(snap.ServerConfig),
		Hash:                snap.Hash,
	}
}

// handleInstallSnapshotRequest applies an incoming snapshot from the leader.
// TODO: Session 4 — full implementation.
func (s *nodeState) handleInstallSnapshotRequest(myID types.ServerID, isa *types.InstallSnapshotArgs) *types.InstallSnapshotResponse {
	s.mu.Lock()
	defer s.mu.Unlock()

	log.Trace("Received InstallSnapshot request")

	if isa.Term < s.currentTerm {
		return &types.InstallSnapshotResponse{
			ID: myID, Term: s.currentTerm, Success: false,
		}
	}
	if isa.Term > s.currentTerm {
		s.updateCurrentTerm(isa.Term)
	}

	// Already have this snapshot or newer — ack without applying.
	if isa.LastIncludedIndex <= s.lastApplied {
		return &types.InstallSnapshotResponse{
			ID:                myID,
			Term:              s.currentTerm,
			Success:           true,
			LastIncludedIndex: isa.LastIncludedIndex,
			LastIncludedTerm:  isa.LastIncludedTerm,
		}
	}

	// Apply the snapshot: discard all log entries, reset state.
	newSnap := &types.Snapshot{
		LastIncludedIndex: isa.LastIncludedIndex,
		LastIncludedTerm:  isa.LastIncludedTerm,
		Data:              isa.Data,
		ServerConfig:      copyConfig(isa.ServerConfiguration),
		Hash:              isa.Hash,
	}
	s.lastSnapshot = newSnap
	s.nextLogIdx = 0
	s.commitIndex = isa.LastIncludedIndex
	s.lastApplied = isa.LastIncludedIndex

	// Signal the RaftNode to restore the state machine from the snapshot.
	s.snapshotInstallChan <- isa.Data

	log.Tracef("State: installed snapshot idx=%d", isa.LastIncludedIndex)
	return &types.InstallSnapshotResponse{
		ID:                myID,
		Term:              s.currentTerm,
		Success:           true,
		LastIncludedIndex: isa.LastIncludedIndex,
		LastIncludedTerm:  isa.LastIncludedTerm,
	}
}

// handleInstallSnapshotResponse processes a reply to an InstallSnapshot RPC.
// Returns matchIndex on success, or -1 on failure.
func (s *nodeState) handleInstallSnapshotResponse(myID types.ServerID, isr *types.InstallSnapshotResponse) int {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !isr.Success {
		if isr.Term >= s.currentTerm {
			log.Info("Become Follower (handleInstallSnapshotResponse)")
			s.role = Follower
			s.updateCurrentTerm(isr.Term)
			s.currentLeader = isr.ID
		}
		return -1
	}

	s.lastSentLogIndex[isr.ID] = isr.LastIncludedIndex
	s.nextIndex[isr.ID] = isr.LastIncludedIndex + 1
	s.matchIndex[isr.ID] = isr.LastIncludedIndex
	return s.matchIndex[isr.ID]
}

// purgeEntriesFromSnapshot removes from AppendEntries args any entries
// already covered by the receiver's snapshot. This is the fix for the
// bug documented in Pozzan & Vardanega Section 3.6.
func (s *nodeState) purgeEntriesFromSnapshot(aea *types.AppendEntriesArgs) {
	if s.lastSnapshot == nil {
		return
	}
	lastSnapIdx := s.lastSnapshot.LastIncludedIndex
	remove := 0
	for i, e := range aea.Entries {
		if e.Index <= lastSnapIdx {
			remove = i + 1
		} else {
			break
		}
	}
	if remove > 0 {
		aea.PrevLogIndex = aea.Entries[remove-1].Index
		aea.PrevLogTerm = aea.Entries[remove-1].Term
		aea.Entries = aea.Entries[remove:]
	}
}

// ---- RaftNode snapshot helpers ------------------------------------

// sendInstallSnapshotRPC sends a snapshot to the given peer asynchronously.
func (n *RaftNode) sendInstallSnapshotRPC(target types.ServerID) {
	if n.state.getLastSnapshot() == nil {
		return
	}
	args := n.state.prepareInstallSnapshotRPC(n.cfg.ID)
	go func() {
		resp, err := n.transport.SendInstallSnapshot(target, args)
		if err != nil {
			log.Debugf("InstallSnapshot to %s: %v", target, err)
			return
		}
		select {
		case n.myISResponseChan <- resp:
		case <-n.stopCh:
		}
	}()
}

// handleInstallSnapshotResponses drains the InstallSnapshot response channel.
func (n *RaftNode) handleInstallSnapshotResponses() {
	for {
		select {
		case resp := <-n.myISResponseChan:
			if n.state.getRole() != Leader {
				continue
			}
			matchIdx := n.state.handleInstallSnapshotResponse(n.cfg.ID, resp)
			if matchIdx >= 0 && matchIdx >= n.state.getCommitIndex() {
				n.promoteUnvotingServer(resp.ID)
			}
		case <-n.stopCh:
			return
		}
	}
}

// snapshotLoop is a goroutine that handles snapshot requests from nodeState
// and snapshot installations from the leader.
func (n *RaftNode) snapshotLoop() {
	for {
		select {
		case <-n.snapshotRequestChan:
			// State machine snapshot requested by takeSnapshot().
			data, err := n.sm.Snapshot()
			if err != nil {
				log.Errorf("StateMachine.Snapshot error: %v", err)
				n.snapshotResponseChan <- nil
			} else {
				n.snapshotResponseChan <- data
			}
		case data := <-n.snapshotInstallChan:
			// Restore from a snapshot installed by the leader.
			if err := n.sm.Restore(data); err != nil {
				log.Errorf("StateMachine.Restore error: %v", err)
			}
		case <-n.stopCh:
			return
		}
	}
}
