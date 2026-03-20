package raft

import (
	"testing"
	"time"

	"github.com/henrique-arab/raft-lib/storage"
	"github.com/henrique-arab/raft-lib/types"
)

// TestStatePersistsAfterApply verifies that after applying a command the
// persistent state (term, logs) is saved to storage and can be recovered.
func TestStatePersistsAfterApply(t *testing.T) {
	net := newMemNetwork()
	id := types.ServerID("n1")
	st := storage.NewMemoryStorage()

	cfg := types.Config{
		ID:            id,
		Peers:         nil,
		HeartbeatMs:   10,
		ElectionMinMs: 50,
		ElectionMaxMs: 100,
	}
	node, err := New(cfg, &nopSM{}, net.add(id), st)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := node.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}

	leader := waitForLeader(t, []*RaftNode{node}, 2*time.Second)
	if leader == nil {
		t.Fatal("no leader")
	}

	resp, err := leader.Apply("c1", 1, []byte("persist-me"))
	if err != nil || !resp.Success {
		t.Fatalf("Apply: err=%v success=%v", err, resp.Success)
	}

	// Give time for persistState to complete.
	time.Sleep(50 * time.Millisecond)
	node.Stop()

	// Verify the storage has persisted state.
	ps, err := st.LoadState()
	if err != nil {
		t.Fatalf("LoadState: %v", err)
	}
	if ps.CurrentTerm < 1 {
		t.Errorf("persisted term=%d, want >=1", ps.CurrentTerm)
	}
	// Should have noop (idx 0) + command (idx 1) = at least 2 entries.
	if len(ps.Logs) < 2 {
		t.Errorf("persisted %d log entries, want >=2", len(ps.Logs))
	}

	// Restart the node with the same storage and verify it recovers.
	net2 := newMemNetwork()
	node2, err := New(cfg, &nopSM{}, net2.add(id), st)
	if err != nil {
		t.Fatalf("New (restart): %v", err)
	}
	if err := node2.Start(); err != nil {
		t.Fatalf("Start (restart): %v", err)
	}
	defer node2.Stop()

	// After restart, node should recover the persisted term.
	node2.state.mu.Lock()
	term := node2.state.currentTerm
	logCount := node2.state.nextLogIdx
	node2.state.mu.Unlock()

	if term < 1 {
		t.Errorf("recovered term=%d, want >=1", term)
	}
	if logCount < 2 {
		t.Errorf("recovered %d log entries, want >=2", logCount)
	}
}
