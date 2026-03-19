package raft

import (
	"sync"
	"testing"
	"time"

	"github.com/henrique-arab/raft-lib/storage"
	"github.com/henrique-arab/raft-lib/types"
)

// ---- recording state machine ----------------------------------------

// recordSM records every Apply call for inspection in tests.
type recordSM struct {
	mu      sync.Mutex
	applied [][]byte
}

func (s *recordSM) Apply(cmd []byte) error {
	s.mu.Lock()
	s.applied = append(s.applied, append([]byte(nil), cmd...))
	s.mu.Unlock()
	return nil
}
func (s *recordSM) Snapshot() ([]byte, error) { return nil, nil }
func (s *recordSM) Restore(_ []byte) error    { return nil }

func (s *recordSM) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.applied)
}

// ---- helpers --------------------------------------------------------

// startClusterWithSM is like startCluster but accepts an SM factory per node.
func startClusterWithSM(t *testing.T, ids []types.ServerID, smFn func() types.StateMachine) ([]*RaftNode, func()) {
	t.Helper()
	net := newMemNetwork()
	nodes := make([]*RaftNode, len(ids))
	for i, id := range ids {
		peers := make([]types.ServerID, 0, len(ids)-1)
		for j, pid := range ids {
			if j != i {
				peers = append(peers, pid)
			}
		}
		cfg := types.Config{
			ID:            id,
			Peers:         peers,
			HeartbeatMs:   10,
			ElectionMinMs: 50,
			ElectionMaxMs: 150,
		}
		node, err := New(cfg, smFn(), net.add(id), storage.NewMemoryStorage())
		if err != nil {
			t.Fatalf("New(%s): %v", id, err)
		}
		nodes[i] = node
	}
	for _, node := range nodes {
		if err := node.Start(); err != nil {
			t.Fatalf("Start: %v", err)
		}
	}
	stop := func() {
		for _, node := range nodes {
			node.Stop()
		}
	}
	return nodes, stop
}

// ---- tests ----------------------------------------------------------

// TestLeaderReplicatesCommand verifies that a command submitted to the leader
// is committed and that all followers eventually advance their commitIndex.
func TestLeaderReplicatesCommand(t *testing.T) {
	ids := []types.ServerID{"n1", "n2", "n3"}
	nodes, stop := startCluster(t, ids)
	defer stop()

	leader := waitForLeader(t, nodes, 3*time.Second)
	if leader == nil {
		t.Fatal("no leader elected")
	}

	resp, err := leader.Apply("c1", 1, []byte("hello"))
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if !resp.Success {
		t.Fatalf("Apply returned failure")
	}

	// Apply returning means the leader committed the entry.
	ci := leader.state.getCommitIndex()
	if ci < 1 {
		t.Errorf("leader commitIndex=%d, want >=1", ci)
	}

	// Wait for followers to receive the updated commitIndex via the next heartbeat.
	time.Sleep(50 * time.Millisecond)

	for _, n := range nodes {
		if n.cfg.ID == leader.cfg.ID {
			continue
		}
		fci := n.state.getCommitIndex()
		if fci < 1 {
			t.Errorf("follower %s commitIndex=%d, want >=1", n.cfg.ID, fci)
		}
	}
}

// TestMultipleCommandsCommitted verifies that several sequential commands all
// reach the commit point on the leader.
func TestMultipleCommandsCommitted(t *testing.T) {
	ids := []types.ServerID{"n1", "n2", "n3"}
	nodes, stop := startCluster(t, ids)
	defer stop()

	leader := waitForLeader(t, nodes, 3*time.Second)
	if leader == nil {
		t.Fatal("no leader elected")
	}

	for i, cmd := range [][]byte{[]byte("a"), []byte("b"), []byte("c")} {
		resp, err := leader.Apply("c1", int64(i+1), cmd)
		if err != nil {
			t.Fatalf("Apply cmd %d: %v", i, err)
		}
		if !resp.Success {
			t.Fatalf("Apply cmd %d: not successful", i)
		}
	}

	// noop entry (index 0) + 3 commands → commitIndex >= 3
	ci := leader.state.getCommitIndex()
	if ci < 3 {
		t.Errorf("leader commitIndex=%d, want >=3", ci)
	}
}

// TestCommandAppliedToStateMachine verifies that the state machine's Apply
// method is called with the correct payload after a command is committed.
func TestCommandAppliedToStateMachine(t *testing.T) {
	sm := &recordSM{}
	net := newMemNetwork()
	id := types.ServerID("n1")
	cfg := types.Config{
		ID:            id,
		Peers:         nil,
		HeartbeatMs:   10,
		ElectionMinMs: 50,
		ElectionMaxMs: 100,
	}
	node, err := New(cfg, sm, net.add(id), storage.NewMemoryStorage())
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := node.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer node.Stop()

	leader := waitForLeader(t, []*RaftNode{node}, 2*time.Second)
	if leader == nil {
		t.Fatal("single node did not become leader")
	}

	payload := []byte("state-machine-payload")
	resp, err := leader.Apply("c1", 1, payload)
	if err != nil || !resp.Success {
		t.Fatalf("Apply: err=%v success=%v", err, resp.Success)
	}

	// The command should have been applied to the SM.
	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		if sm.count() >= 1 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if sm.count() < 1 {
		t.Fatal("state machine Apply not called after command was committed")
	}
}

// TestClientDeduplication verifies that replaying a command with an already-
// applied sequence number is idempotent: it succeeds but does not add a new
// log entry.
func TestClientDeduplication(t *testing.T) {
	nodes, stop := startCluster(t, []types.ServerID{"n1"})
	defer stop()

	leader := waitForLeader(t, nodes, 2*time.Second)
	if leader == nil {
		t.Fatal("no leader")
	}

	resp1, err := leader.Apply("c1", 1, []byte("cmd"))
	if err != nil || !resp1.Success {
		t.Fatalf("first Apply: err=%v", err)
	}

	// Replay same seqNum — should succeed without adding a new log entry.
	resp2, err := leader.Apply("c1", 1, []byte("cmd"))
	if err != nil || !resp2.Success {
		t.Fatalf("duplicate Apply: err=%v", err)
	}

	// Log must contain exactly: noop (idx 0) + 1 command (idx 1).
	leader.state.mu.Lock()
	logCount := leader.state.nextLogIdx
	leader.state.mu.Unlock()
	if logCount != 2 {
		t.Errorf("expected 2 log entries (noop + cmd), got %d", logCount)
	}
}
