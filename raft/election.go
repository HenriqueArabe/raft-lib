package raft

import (
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/henrique-arab/raft-lib/types"
)

// ---- nodeState election methods ------------------------------------

// startElection transitions the node to Candidate and begins a new term.
// Rule: increment currentTerm, vote for self, reset vote counter.
func (s *nodeState) startElection(id types.ServerID) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.currentTerm++
	log.Infof("Become Candidate: term=%d", s.currentTerm)
	s.role = Candidate
	s.votedFor = id
	s.electionVotes = 1
}

// prepareRequestVoteRPC builds the RequestVote args from current state.
func (s *nodeState) prepareRequestVoteRPC(id types.ServerID) *types.RequestVoteArgs {
	s.mu.Lock()
	defer s.mu.Unlock()
	lastIdx, lastTerm := s.getLastLogIdxTerm()
	return &types.RequestVoteArgs{
		Term:         s.currentTerm,
		CandidateID:  id,
		LastLogIndex: lastIdx,
		LastLogTerm:  lastTerm,
	}
}

// handleRequestToVote processes an incoming RequestVote RPC.
// Returns the vote response; may transition the node to Follower.
func (s *nodeState) handleRequestToVote(id types.ServerID, rva *types.RequestVoteArgs) *types.RequestVoteResponse {
	s.mu.Lock()
	defer s.mu.Unlock()

	lastIdx, lastTerm := s.getLastLogIdxTerm()

	// Deny if our term is newer.
	if s.currentTerm > rva.Term {
		log.Tracef("Reject vote: our term %d > candidate term %d", s.currentTerm, rva.Term)
		return &types.RequestVoteResponse{ID: id, Term: s.currentTerm, VoteGranted: false}
	}

	// Step down if we see a newer term.
	if s.currentTerm < rva.Term {
		s.updateCurrentTerm(rva.Term)
	}

	// Grant vote if not yet voted (or already voted for this candidate) AND
	// the candidate's log is at least as up-to-date as ours.
	canVote := s.votedFor == "" || s.votedFor == rva.CandidateID
	logOK := rva.LastLogTerm > lastTerm ||
		(rva.LastLogTerm == lastTerm && rva.LastLogIndex >= lastIdx)

	if canVote && logOK {
		s.stopElectionTimeoutLocked()
		log.Infof("Become Follower (handleRequestToVote): voted for %s", rva.CandidateID)
		s.role = Follower
		s.votedFor = rva.CandidateID
		s.currentLeader = rva.CandidateID
		return &types.RequestVoteResponse{ID: id, Term: s.currentTerm, VoteGranted: true}
	}

	log.Tracef("Reject vote: votedFor=%s lastTerm=%d/%d lastIdx=%d/%d",
		s.votedFor, lastTerm, rva.LastLogTerm, lastIdx, rva.LastLogIndex)
	return &types.RequestVoteResponse{ID: id, Term: s.currentTerm, VoteGranted: false}
}

// updateElection processes a RequestVote response.
// Returns true if the node has just won the election.
func (s *nodeState) updateElection(resp *types.RequestVoteResponse) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Step down if we see a newer term.
	if resp.Term > s.currentTerm {
		s.stopElectionTimeoutLocked()
		s.electionVotes = 0
		s.updateCurrentTerm(resp.Term)
		log.Info("Become Follower (updateElection): stale term")
		s.role = Follower
		return false
	}

	// Only count votes for the current term.
	if resp.Term == s.currentTerm && resp.VoteGranted {
		s.electionVotes++
		log.Tracef("Election votes: %d / %d", s.electionVotes, len(s.serverConfig))
		if s.electionVotes > len(s.serverConfig)/2 {
			return true // caller must call winElection outside the lock
		}
	}
	return false
}

// winElection transitions the node to Leader, resets leader-only state,
// and appends the mandatory no-op entry.
func (s *nodeState) winElection(id types.ServerID) {
	s.mu.Lock()
	defer s.mu.Unlock()

	log.Infof("Become Leader: term=%d", s.currentTerm)
	lastIdx, _ := s.getLastLogIdxTerm()
	s.electionVotes = 0
	s.role = Leader
	s.currentLeader = id

	for sid := range s.nextIndex {
		s.nextIndex[sid] = lastIdx + 1
	}
	s.matchIndex[id] = -1
	s.lastSentLogIndex[id] = -1

	s.addNoopLogLocked(id)
}

// ---- RaftNode election helpers -------------------------------------

// sendRequestVoteRPCs issues RequestVote RPCs to all voting peers in parallel.
// Responses are routed to n.myVoteResponseChan.
func (n *RaftNode) sendRequestVoteRPCs(args *types.RequestVoteArgs) {
	for _, peer := range n.cfg.Peers {
		peer := peer
		go func() {
			resp, err := n.transport.SendRequestVote(peer, args)
			if err != nil {
				log.Debugf("RequestVote to %s: %v", peer, err)
				return
			}
			select {
			case n.myVoteResponseChan <- resp:
			case <-time.After(time.Duration(n.cfg.ElectionMinMs) * time.Millisecond):
				log.Warnf("RequestVote response from %s not consumed", peer)
			case <-n.stopCh:
			}
		}()
	}
}
