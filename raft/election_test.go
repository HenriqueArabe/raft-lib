package raft

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/henrique-arab/raft-lib/storage"
	"github.com/henrique-arab/raft-lib/transport"
	"github.com/henrique-arab/raft-lib/types"
)

// ---- in-memory transport ----------------------------------------

// memNetwork is a virtual network that connects memTransport instances in
// the same process. Used for deterministic, fast unit tests.
type memNetwork struct {
	mu    sync.RWMutex
	nodes map[types.ServerID]*memTransport
}

func newMemNetwork() *memNetwork {
	return &memNetwork{nodes: make(map[types.ServerID]*memTransport)}
}

// add creates a new memTransport for id and registers it in the network.
func (net *memNetwork) add(id types.ServerID) *memTransport {
	t := &memTransport{id: id, net: net}
	net.mu.Lock()
	net.nodes[id] = t
	net.mu.Unlock()
	return t
}

type memTransport struct {
	id  types.ServerID
	net *memNetwork
	mu  sync.RWMutex
	h   transport.TransportHandler
}

func (t *memTransport) RegisterHandler(h transport.TransportHandler) {
	t.mu.Lock()
	t.h = h
	t.mu.Unlock()
}

func (t *memTransport) peer(id types.ServerID) transport.TransportHandler {
	t.net.mu.RLock()
	p := t.net.nodes[id]
	t.net.mu.RUnlock()
	if p == nil {
		return nil
	}
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.h
}

func (t *memTransport) SendRequestVote(target types.ServerID, args *types.RequestVoteArgs) (*types.RequestVoteResponse, error) {
	h := t.peer(target)
	if h == nil {
		return nil, fmt.Errorf("no handler for %s", target)
	}
	return h.HandleRequestVote(args), nil
}

func (t *memTransport) SendAppendEntries(target types.ServerID, args *types.AppendEntriesArgs) (*types.AppendEntriesResponse, error) {
	h := t.peer(target)
	if h == nil {
		return nil, fmt.Errorf("no handler for %s", target)
	}
	return h.HandleAppendEntries(args), nil
}

func (t *memTransport) SendInstallSnapshot(target types.ServerID, args *types.InstallSnapshotArgs) (*types.InstallSnapshotResponse, error) {
	h := t.peer(target)
	if h == nil {
		return nil, fmt.Errorf("no handler for %s", target)
	}
	return h.HandleInstallSnapshot(args), nil
}

func (t *memTransport) SendApply(target types.ServerID, args *types.ApplyArgs) (*types.ApplyResponse, error) {
	h := t.peer(target)
	if h == nil {
		return nil, fmt.Errorf("no handler for %s", target)
	}
	return h.HandleApply(args), nil
}

func (t *memTransport) SendAddRemoveServer(target types.ServerID, args *types.AddRemoveServerArgs) (*types.AddRemoveServerResponse, error) {
	h := t.peer(target)
	if h == nil {
		return nil, fmt.Errorf("no handler for %s", target)
	}
	return h.HandleAddRemoveServer(args), nil
}

func (t *memTransport) Listen(_ string) error            { return nil }
func (t *memTransport) Connect(_ types.ServerID) error   { return nil }
func (t *memTransport) Disconnect(_ types.ServerID) error { return nil }
func (t *memTransport) Close() error                     { return nil }

var _ transport.Transport = (*memTransport)(nil)

// ---- no-op state machine ----------------------------------------

type nopSM struct{}

func (s *nopSM) Apply(_ []byte) error      { return nil }
func (s *nopSM) Snapshot() ([]byte, error) { return nil, nil }
func (s *nopSM) Restore(_ []byte) error    { return nil }

// ---- helpers ----------------------------------------------------

func startCluster(t *testing.T, ids []types.ServerID) ([]*RaftNode, func()) {
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
		node, err := New(cfg, &nopSM{}, net.add(id), storage.NewMemoryStorage())
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

func waitForLeader(t *testing.T, nodes []*RaftNode, timeout time.Duration) *RaftNode {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for _, n := range nodes {
			if n.state.getRole() == Leader {
				return n
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	return nil
}

// ---- tests -------------------------------------------------------

func TestSingleNodeBecomesLeader(t *testing.T) {
	nodes, stop := startCluster(t, []types.ServerID{"n1"})
	defer stop()

	leader := waitForLeader(t, nodes, 2*time.Second)
	if leader == nil {
		t.Fatal("node did not become leader within timeout")
	}
}

func TestThreeNodeElectsOneLeader(t *testing.T) {
	ids := []types.ServerID{"n1", "n2", "n3"}
	nodes, stop := startCluster(t, ids)
	defer stop()

	leader := waitForLeader(t, nodes, 3*time.Second)
	if leader == nil {
		t.Fatal("no leader elected within timeout")
	}

	// Verify exactly one leader.
	leaders := 0
	for _, n := range nodes {
		if n.state.getRole() == Leader {
			leaders++
		}
	}
	if leaders != 1 {
		t.Errorf("expected 1 leader, got %d", leaders)
	}
}

func TestLeaderSendsHeartbeats(t *testing.T) {
	ids := []types.ServerID{"n1", "n2", "n3"}
	nodes, stop := startCluster(t, ids)
	defer stop()

	leader := waitForLeader(t, nodes, 3*time.Second)
	if leader == nil {
		t.Fatal("no leader elected within timeout")
	}

	// Wait two heartbeat intervals and verify followers stay as followers
	// (i.e. they are receiving heartbeats and not starting new elections).
	time.Sleep(100 * time.Millisecond)

	for _, n := range nodes {
		if n.cfg.ID == leader.cfg.ID {
			continue
		}
		if n.state.getRole() == Candidate {
			t.Errorf("follower %s became Candidate — leader may have stopped heartbeating", n.cfg.ID)
		}
	}
}
