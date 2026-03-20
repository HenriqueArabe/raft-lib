package raft

import (
	"fmt"
	"testing"
	"time"

	"github.com/henrique-arab/raft-lib/storage"
	"github.com/henrique-arab/raft-lib/transport"
	"github.com/henrique-arab/raft-lib/types"
)

// TestTCPClusterElectsLeader verifies that a 3-node cluster using real
// RPCTransport over TCP can elect a leader.
func TestTCPClusterElectsLeader(t *testing.T) {
	const n = 3
	transports := make([]*transport.RPCTransport, n)
	addrs := make([]string, n)

	// Start listeners on random ports.
	for i := 0; i < n; i++ {
		id := types.ServerID(fmt.Sprintf("node%d", i))
		tr := transport.NewRPCTransport(id)
		transports[i] = tr
	}

	// We need to listen first to discover actual addresses.
	for i := 0; i < n; i++ {
		if err := transports[i].Listen(":0"); err != nil {
			t.Fatalf("Listen node%d: %v", i, err)
		}
		addrs[i] = transports[i].Addr().String()
	}
	// Now close listeners — we'll re-create transports with correct IDs (the addr).
	for i := 0; i < n; i++ {
		transports[i].Close()
	}

	// Re-create transports with address as ServerID (as Raft expects).
	ids := make([]types.ServerID, n)
	for i := 0; i < n; i++ {
		ids[i] = types.ServerID(addrs[i])
	}

	transports2 := make([]*transport.RPCTransport, n)
	for i := 0; i < n; i++ {
		transports2[i] = transport.NewRPCTransport(ids[i])
	}

	nodes := make([]*RaftNode, n)
	for i := 0; i < n; i++ {
		peers := make([]types.ServerID, 0, n-1)
		for j := 0; j < n; j++ {
			if j != i {
				peers = append(peers, ids[j])
			}
		}
		cfg := types.Config{
			ID:            ids[i],
			Peers:         peers,
			HeartbeatMs:   20,
			ElectionMinMs: 150,
			ElectionMaxMs: 300,
		}
		node, err := New(cfg, &nopSM{}, transports2[i], storage.NewMemoryStorage())
		if err != nil {
			t.Fatalf("New node%d: %v", i, err)
		}
		nodes[i] = node
	}

	for i, node := range nodes {
		if err := node.Start(); err != nil {
			t.Fatalf("Start node%d: %v", i, err)
		}
	}
	defer func() {
		for _, node := range nodes {
			node.Stop()
		}
	}()

	leader := waitForLeader(t, nodes, 5*time.Second)
	if leader == nil {
		t.Fatal("no leader elected over TCP")
	}

	// Verify exactly one leader.
	leaders := 0
	for _, nd := range nodes {
		if nd.state.getRole() == Leader {
			leaders++
		}
	}
	if leaders != 1 {
		t.Errorf("expected 1 leader, got %d", leaders)
	}
}

// TestTCPClusterReplicatesCommand verifies that a command submitted to the
// leader over TCP is committed and replicated to followers.
func TestTCPClusterReplicatesCommand(t *testing.T) {
	const n = 3
	addrs := make([]string, n)

	// Discover free ports.
	tmpTransports := make([]*transport.RPCTransport, n)
	for i := 0; i < n; i++ {
		tr := transport.NewRPCTransport(types.ServerID(fmt.Sprintf("tmp%d", i)))
		if err := tr.Listen(":0"); err != nil {
			t.Fatalf("Listen tmp%d: %v", i, err)
		}
		addrs[i] = tr.Addr().String()
		tmpTransports[i] = tr
	}
	for _, tr := range tmpTransports {
		tr.Close()
	}

	ids := make([]types.ServerID, n)
	transports := make([]*transport.RPCTransport, n)
	for i := 0; i < n; i++ {
		ids[i] = types.ServerID(addrs[i])
		transports[i] = transport.NewRPCTransport(ids[i])
	}

	sms := make([]*recordSM, n)
	nodes := make([]*RaftNode, n)
	for i := 0; i < n; i++ {
		peers := make([]types.ServerID, 0, n-1)
		for j := 0; j < n; j++ {
			if j != i {
				peers = append(peers, ids[j])
			}
		}
		sms[i] = &recordSM{}
		cfg := types.Config{
			ID:            ids[i],
			Peers:         peers,
			HeartbeatMs:   20,
			ElectionMinMs: 150,
			ElectionMaxMs: 300,
		}
		node, err := New(cfg, sms[i], transports[i], storage.NewMemoryStorage())
		if err != nil {
			t.Fatalf("New node%d: %v", i, err)
		}
		nodes[i] = node
	}

	for i, node := range nodes {
		if err := node.Start(); err != nil {
			t.Fatalf("Start node%d: %v", i, err)
		}
	}
	defer func() {
		for _, node := range nodes {
			node.Stop()
		}
	}()

	leader := waitForLeader(t, nodes, 5*time.Second)
	if leader == nil {
		t.Fatal("no leader elected over TCP")
	}

	resp, err := leader.Apply("client1", 1, []byte("hello-tcp"))
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if !resp.Success {
		t.Fatal("Apply returned failure")
	}

	ci := leader.state.getCommitIndex()
	if ci < 1 {
		t.Errorf("leader commitIndex=%d, want >=1", ci)
	}

	// Wait for followers to receive the commit via heartbeats.
	time.Sleep(200 * time.Millisecond)

	for i, nd := range nodes {
		if nd.cfg.ID == leader.cfg.ID {
			continue
		}
		fci := nd.state.getCommitIndex()
		if fci < 1 {
			t.Errorf("follower %d commitIndex=%d, want >=1", i, fci)
		}
	}
}
