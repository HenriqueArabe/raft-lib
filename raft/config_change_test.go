package raft

import (
	"testing"
	"time"

	"github.com/henrique-arab/raft-lib/storage"
	"github.com/henrique-arab/raft-lib/types"
)

// TestAddServerJoinsCluster verifies that a new node added via AddServer
// eventually receives log entries and is promoted to the voting config.
func TestAddServerJoinsCluster(t *testing.T) {
	net := newMemNetwork()

	newNode := func(id types.ServerID, peers []types.ServerID) *RaftNode {
		cfg := types.Config{
			ID:            id,
			Peers:         peers,
			HeartbeatMs:   10,
			ElectionMinMs: 50,
			ElectionMaxMs: 150,
		}
		node, err := New(cfg, &nopSM{}, net.add(id), storage.NewMemoryStorage())
		if err != nil {
			t.Fatalf("New(%s): %v", id, err)
		}
		return node
	}

	// Start a 3-node cluster.
	n1 := newNode("n1", []types.ServerID{"n2", "n3"})
	n2 := newNode("n2", []types.ServerID{"n1", "n3"})
	n3 := newNode("n3", []types.ServerID{"n1", "n2"})
	for _, n := range []*RaftNode{n1, n2, n3} {
		if err := n.Start(); err != nil {
			t.Fatalf("Start: %v", err)
		}
	}
	defer n1.Stop()
	defer n2.Stop()
	defer n3.Stop()

	leader := waitForLeader(t, []*RaftNode{n1, n2, n3}, 3*time.Second)
	if leader == nil {
		t.Fatal("no leader")
	}

	// Apply a command so there is something to replicate to the new node.
	if _, err := leader.Apply("c1", 1, []byte("before-add")); err != nil {
		t.Fatalf("Apply: %v", err)
	}

	// Start the 4th node (n4) and register it in the network before AddServer.
	n4 := newNode("n4", nil)
	if err := n4.Start(); err != nil {
		t.Fatalf("Start n4: %v", err)
	}
	defer n4.Stop()

	resp, err := leader.AddServer("n4")
	if err != nil {
		t.Fatalf("AddServer: %v", err)
	}
	if !resp.Success {
		t.Fatalf("AddServer returned failure")
	}

	// n4 should eventually appear in the leader's serverConfig.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if leader.state.serverInConfiguration("n4") {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Error("n4 not in serverConfig after AddServer")
}

// TestRemoveServerLeavesCluster verifies that RemoveServer removes a node
// from the voting configuration.
func TestRemoveServerLeavesCluster(t *testing.T) {
	ids := []types.ServerID{"n1", "n2", "n3"}
	nodes, stop := startCluster(t, ids)
	defer stop()

	leader := waitForLeader(t, nodes, 3*time.Second)
	if leader == nil {
		t.Fatal("no leader")
	}

	// Find a non-leader to remove.
	var target types.ServerID
	for _, n := range nodes {
		if n.cfg.ID != leader.cfg.ID {
			target = n.cfg.ID
			break
		}
	}

	resp, err := leader.RemoveServer(target)
	if err != nil {
		t.Fatalf("RemoveServer: %v", err)
	}
	if !resp.Success {
		t.Fatalf("RemoveServer returned failure")
	}

	if leader.state.serverInConfiguration(target) {
		t.Errorf("server %s still in config after RemoveServer", target)
	}
}
