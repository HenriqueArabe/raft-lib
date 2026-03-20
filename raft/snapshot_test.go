package raft

import (
	"testing"
	"time"

	"github.com/henrique-arab/raft-lib/storage"
	"github.com/henrique-arab/raft-lib/types"
)

// snapshotSM is a state machine that supports Snapshot/Restore for testing.
type snapshotSM struct {
	recordSM
	snapData []byte
}

func (s *snapshotSM) Snapshot() ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]byte, len(s.snapData))
	copy(out, s.snapData)
	return out, nil
}

func (s *snapshotSM) Restore(data []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.snapData = append([]byte(nil), data...)
	return nil
}

func (s *snapshotSM) Apply(cmd []byte) error {
	s.mu.Lock()
	s.applied = append(s.applied, append([]byte(nil), cmd...))
	s.snapData = append(s.snapData, cmd...)
	s.mu.Unlock()
	return nil
}

// TestSnapshotCompactsLog verifies that takeSnapshot reduces nextLogIdx and
// preserves lastSnapshot.
func TestSnapshotCompactsLog(t *testing.T) {
	ids := []types.ServerID{"n1"}
	nodes, stop := startClusterWithSM(t, ids, func() types.StateMachine { return &snapshotSM{} })
	defer stop()

	leader := waitForLeader(t, nodes, 2*time.Second)
	if leader == nil {
		t.Fatal("no leader")
	}

	// Apply several commands so we have entries to compact.
	for i := 0; i < 5; i++ {
		resp, err := leader.Apply("c1", int64(i+1), []byte("data"))
		if err != nil || !resp.Success {
			t.Fatalf("Apply %d: err=%v success=%v", i, err, resp.Success)
		}
	}

	// Force a snapshot via the state method (simulates log-full trigger).
	leader.state.mu.Lock()
	before := leader.state.nextLogIdx
	ok := leader.state.takeSnapshot()
	after := leader.state.nextLogIdx
	snap := leader.state.lastSnapshot
	leader.state.mu.Unlock()

	if !ok {
		t.Fatal("takeSnapshot returned false")
	}
	if snap == nil {
		t.Fatal("lastSnapshot is nil after takeSnapshot")
	}
	if after >= before {
		t.Errorf("nextLogIdx not compacted: before=%d after=%d", before, after)
	}
}

// TestInstallSnapshotFollower verifies that a follower accepts an
// InstallSnapshot RPC and advances its lastApplied/commitIndex.
func TestInstallSnapshotFollower(t *testing.T) {
	net := newMemNetwork()

	newNode := func(id types.ServerID, peers []types.ServerID) *RaftNode {
		cfg := types.Config{
			ID:            id,
			Peers:         peers,
			HeartbeatMs:   10,
			ElectionMinMs: 50,
			ElectionMaxMs: 150,
		}
		node, err := New(cfg, &snapshotSM{}, net.add(id), storage.NewMemoryStorage())
		if err != nil {
			t.Fatalf("New(%s): %v", id, err)
		}
		return node
	}

	n1 := newNode("n1", []types.ServerID{"n2"})
	n2 := newNode("n2", []types.ServerID{"n1"})
	for _, n := range []*RaftNode{n1, n2} {
		if err := n.Start(); err != nil {
			t.Fatalf("Start: %v", err)
		}
	}
	defer n1.Stop()
	defer n2.Stop()

	// Build a snapshot args directly and send to n2.
	args := &types.InstallSnapshotArgs{
		ID:                  "n1",
		Term:                1,
		LastIncludedIndex:   10,
		LastIncludedTerm:    1,
		Data:                []byte("snapshot-payload"),
		ServerConfiguration: map[types.ServerID]bool{"n1": true, "n2": true},
	}

	// Update n2's term so the snapshot is accepted.
	n2.state.mu.Lock()
	n2.state.currentTerm = 1
	n2.state.mu.Unlock()

	resp := n2.state.handleInstallSnapshotRequest("n2", args)
	if !resp.Success {
		t.Fatal("handleInstallSnapshotRequest returned failure")
	}

	n2.state.mu.Lock()
	ci := n2.state.commitIndex
	la := n2.state.lastApplied
	n2.state.mu.Unlock()

	if ci != 10 || la != 10 {
		t.Errorf("after snapshot: commitIndex=%d lastApplied=%d, want both 10", ci, la)
	}
}

// TestSendInstallSnapshotRPC verifies that sendInstallSnapshotRPC delivers
// the snapshot to the target and the response is processed.
func TestSendInstallSnapshotRPC(t *testing.T) {
	net2 := newMemNetwork()

	newNode2 := func(id types.ServerID, peers []types.ServerID) *RaftNode {
		cfg := types.Config{
			ID:            id,
			Peers:         peers,
			HeartbeatMs:   10,
			ElectionMinMs: 50,
			ElectionMaxMs: 150,
		}
		node, err := New(cfg, &snapshotSM{}, net2.add(id), storage.NewMemoryStorage())
		if err != nil {
			t.Fatalf("New(%s): %v", id, err)
		}
		return node
	}

	n1 := newNode2("n1", []types.ServerID{"n2"})
	n2 := newNode2("n2", []types.ServerID{"n1"})
	for _, n := range []*RaftNode{n1, n2} {
		if err := n.Start(); err != nil {
			t.Fatalf("Start: %v", err)
		}
	}
	defer n1.Stop()
	defer n2.Stop()

	// Set up n1 as leader with a snapshot.
	n1.state.mu.Lock()
	n1.state.role = Leader
	n1.state.currentLeader = "n1"
	n1.state.currentTerm = 1
	n1.state.lastSnapshot = &types.Snapshot{
		LastIncludedIndex: 5,
		LastIncludedTerm:  1,
		Data:              []byte("snap"),
		ServerConfig:      map[types.ServerID]bool{"n1": true, "n2": true},
	}
	n1.state.mu.Unlock()

	// Set n2's term to match so the snapshot is accepted.
	n2.state.mu.Lock()
	n2.state.currentTerm = 1
	n2.state.mu.Unlock()

	n1.sendInstallSnapshotRPC("n2")

	// Give the async goroutine time to complete and process the response.
	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		n2.state.mu.Lock()
		la := n2.state.lastApplied
		n2.state.mu.Unlock()
		if la >= 5 {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	n2.state.mu.Lock()
	la := n2.state.lastApplied
	n2.state.mu.Unlock()
	t.Errorf("n2 lastApplied=%d, want >=5 after InstallSnapshot", la)
}
