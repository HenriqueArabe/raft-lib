package raft

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"math/rand"
	"sync"
	"time"

	"github.com/henrique-arab/raft-lib/types"
)

// nodeRole represents the Raft role of a node at any given time.
type nodeRole int

const (
	Follower  nodeRole = 0
	Candidate nodeRole = 1
	Leader    nodeRole = 2
)

const logArrayCapacity = 1024

// pendingCommand tracks an in-flight client command awaiting commit.
type pendingCommand struct {
	logIndex int
	resultCh chan error
}

// configEntry wraps a pending membership change queued on the leader.
type configEntry struct {
	msg         types.ConfigChange
	chanApplied chan bool
	resultCh    chan *types.AddRemoveServerResponse
}

// nodeState holds all mutable Raft state for a single node.
// All fields are protected by mu unless noted.
type nodeState struct {
	mu sync.Mutex

	// --- Persistent state (must survive crashes) ---
	currentTerm int
	votedFor    types.ServerID
	logs        [logArrayCapacity]types.RaftLog
	logHashes   [logArrayCapacity][32]byte
	nextLogIdx  int // next free index in logs[]

	// --- Volatile state ---
	role          nodeRole
	commitIndex   int
	lastApplied   int
	currentLeader types.ServerID
	lastSnapshot  *types.Snapshot

	// --- Leader-only volatile state ---
	nextIndex        map[types.ServerID]int
	matchIndex       map[types.ServerID]int
	lastSentLogIndex map[types.ServerID]int

	// --- Election ---
	electionTimeoutStarted bool
	electionTimer          *time.Timer
	electionVotes          int

	// --- Cluster configuration ---
	serverConfig       map[types.ServerID]bool
	oldServerConfig    map[types.ServerID]bool
	lastConfigLogIndex int
	inConfigChange     bool
	pendingConfigs     int
	configQueue        []configEntry
	unvotingServers    map[types.ServerID]configEntry

	// --- Client deduplication ---
	clientLastSeq map[types.ServerID]int64

	// --- Snapshot channels (owned by RaftNode, shared here) ---
	snapshotRequestChan  chan struct{}
	snapshotResponseChan chan []byte
	snapshotInstallChan  chan []byte

	// --- Config (read-only after construction) ---
	electionMinMs int
	electionMaxMs int
}

func newNodeState(
	id types.ServerID,
	peers []types.ServerID,
	electionMinMs, electionMaxMs int,
	snapshotReqChan chan struct{},
	snapshotRespChan chan []byte,
	snapshotInstallChan chan []byte,
) *nodeState {
	s := &nodeState{
		currentTerm:          0,
		votedFor:             "",
		nextLogIdx:           0,
		role:                 Follower,
		commitIndex:          -1,
		lastApplied:          -1,
		currentLeader:        "",
		nextIndex:            make(map[types.ServerID]int),
		matchIndex:           make(map[types.ServerID]int),
		lastSentLogIndex:     make(map[types.ServerID]int),
		serverConfig:         map[types.ServerID]bool{id: true},
		oldServerConfig:      map[types.ServerID]bool{id: true},
		lastConfigLogIndex:   0,
		configQueue:          []configEntry{},
		unvotingServers:      make(map[types.ServerID]configEntry),
		clientLastSeq:        make(map[types.ServerID]int64),
		snapshotRequestChan:  snapshotReqChan,
		snapshotResponseChan: snapshotRespChan,
		snapshotInstallChan:  snapshotInstallChan,
		electionMinMs:        electionMinMs,
		electionMaxMs:        electionMaxMs,
	}
	for _, p := range peers {
		s.nextIndex[p] = 0
		s.matchIndex[p] = -1
		s.lastSentLogIndex[p] = -1
		s.serverConfig[p] = true
		s.oldServerConfig[p] = true
	}
	return s
}

// ---- Timer helpers -------------------------------------------------

// checkElectionTimeout starts (or returns the already-running) election timer.
func (s *nodeState) checkElectionTimeout() *time.Timer {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.electionTimeoutStarted {
		d := time.Duration(rand.Intn(s.electionMaxMs-s.electionMinMs)+s.electionMinMs) * time.Millisecond
		s.electionTimeoutStarted = true
		s.electionTimer = time.NewTimer(d)
	}
	return s.electionTimer
}

// stopElectionTimeout cancels the election timer and drains its channel.
func (s *nodeState) stopElectionTimeout() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.stopElectionTimeoutLocked()
}

// stopElectionTimeoutLocked is like stopElectionTimeout but mu must be held.
func (s *nodeState) stopElectionTimeoutLocked() {
	if s.electionTimer != nil {
		if !s.electionTimer.Stop() {
			select {
			case <-s.electionTimer.C:
			default:
			}
		}
		s.electionTimeoutStarted = false
	}
}

// ---- Simple getters ------------------------------------------------

func (s *nodeState) getRole() nodeRole {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.role
}

func (s *nodeState) getCurrentLeader() types.ServerID {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.currentLeader
}

func (s *nodeState) getCommitIndex() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.commitIndex
}

func (s *nodeState) getClientLastSeq(id types.ServerID) int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.clientLastSeq[id]
}

func (s *nodeState) setClientLastSeq(id types.ServerID, seq int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.clientLastSeq[id] = seq
}

// ---- Log array helpers ---------------------------------------------

// getLastLogIdxTerm returns the index and term of the last log entry
// (or from the last snapshot if the log array is empty).
func (s *nodeState) getLastLogIdxTerm() (int, int) {
	if s.nextLogIdx > 0 {
		last := s.logs[s.nextLogIdx-1]
		return last.Index, last.Term
	}
	if s.lastSnapshot != nil {
		return s.lastSnapshot.LastIncludedIndex, s.lastSnapshot.LastIncludedTerm
	}
	return -1, -1
}

// findArrayIdxByLogIndex returns the array position of the log entry with
// the given logical index, scanning backwards from the newest entry.
func (s *nodeState) findArrayIdxByLogIndex(logIdx int) (bool, int) {
	for i := s.nextLogIdx - 1; i >= 0; i-- {
		if s.logs[i].Index == logIdx {
			return true, i
		}
	}
	return false, -1
}

// getLogHash computes the chained SHA-256 hash for raftLog.
// Each entry's hash covers its own bytes plus the previous entry's hash
// (or the snapshot hash for the first entry after a snapshot).
func (s *nodeState) getLogHash(rl types.RaftLog) [32]byte {
	b := raftLogBytes(rl)
	_, arrIdx := s.findArrayIdxByLogIndex(rl.Index)
	if arrIdx > 0 {
		return sha256.Sum256(append(b, s.logHashes[arrIdx-1][:]...))
	}
	if s.lastSnapshot != nil {
		return sha256.Sum256(append(b, s.lastSnapshot.Hash[:]...))
	}
	return sha256.Sum256(b)
}

// copyLogsToBeginning shifts logs[startLog:] to the beginning of the array
// after a snapshot has been taken.
func (s *nodeState) copyLogsToBeginning(startLog int) {
	dst := 0
	for startLog+dst < s.nextLogIdx {
		s.logs[dst] = s.logs[startLog+dst]
		s.logHashes[dst] = s.logHashes[startLog+dst]
		dst++
	}
	s.nextLogIdx = dst
}

// updateCurrentTerm sets a new term and clears the voted-for field.
// mu must be held by the caller.
func (s *nodeState) updateCurrentTerm(term int) {
	s.currentTerm = term
	s.votedFor = ""
}

// ---- Serialisation helper -----------------------------------------

// raftLogBytes serializes the fields of a RaftLog that are covered by the hash.
func raftLogBytes(rl types.RaftLog) []byte {
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.LittleEndian, int64(rl.Index))
	binary.Write(buf, binary.LittleEndian, int64(rl.Term))
	binary.Write(buf, binary.LittleEndian, int64(rl.Type))
	buf.Write(rl.Data)
	binary.Write(buf, binary.LittleEndian, rl.Config.Add)
	buf.WriteString(string(rl.Config.Server))
	return buf.Bytes()
}
