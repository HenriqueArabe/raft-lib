package raft

import (
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/henrique-arab/raft-lib/storage"
	"github.com/henrique-arab/raft-lib/types"
)

// ---- kv state machine for tests ------------------------------------

type kvOp struct {
	Kind  string `json:"kind"`
	Key   string `json:"key"`
	Value string `json:"value,omitempty"`
}

type kvSM struct {
	mu   sync.RWMutex
	data map[string]string
}

func newKVSM() *kvSM { return &kvSM{data: make(map[string]string)} }

func (kv *kvSM) Apply(cmd []byte) error {
	var op kvOp
	if err := json.Unmarshal(cmd, &op); err != nil {
		return err
	}
	kv.mu.Lock()
	defer kv.mu.Unlock()
	switch op.Kind {
	case "set":
		kv.data[op.Key] = op.Value
	case "del":
		delete(kv.data, op.Key)
	}
	return nil
}

func (kv *kvSM) Snapshot() ([]byte, error) {
	kv.mu.RLock()
	defer kv.mu.RUnlock()
	return json.Marshal(kv.data)
}

func (kv *kvSM) Restore(data []byte) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	kv.data = make(map[string]string)
	return json.Unmarshal(data, &kv.data)
}

func (kv *kvSM) get(key string) (string, bool) {
	kv.mu.RLock()
	defer kv.mu.RUnlock()
	v, ok := kv.data[key]
	return v, ok
}

// ---- helpers -------------------------------------------------------

func kvCmd(kind, key, value string) []byte {
	data, _ := json.Marshal(kvOp{Kind: kind, Key: key, Value: value})
	return data
}

// ---- tests ---------------------------------------------------------

// TestKVStateMachineApply verifies that the KV state machine correctly
// processes set and del operations through the Raft cluster.
func TestKVStateMachineApply(t *testing.T) {
	sms := make([]*kvSM, 3)
	ids := []types.ServerID{"n1", "n2", "n3"}

	net := newMemNetwork()
	nodes := make([]*RaftNode, 3)
	for i, id := range ids {
		peers := make([]types.ServerID, 0, 2)
		for j, pid := range ids {
			if j != i {
				peers = append(peers, pid)
			}
		}
		sms[i] = newKVSM()
		cfg := types.Config{
			ID: id, Peers: peers,
			HeartbeatMs: 10, ElectionMinMs: 50, ElectionMaxMs: 150,
		}
		node, err := New(cfg, sms[i], net.add(id), storage.NewMemoryStorage())
		if err != nil {
			t.Fatalf("New(%s): %v", id, err)
		}
		nodes[i] = node
	}
	for _, n := range nodes {
		if err := n.Start(); err != nil {
			t.Fatalf("Start: %v", err)
		}
	}
	defer func() {
		for _, n := range nodes {
			n.Stop()
		}
	}()

	leader := waitForLeader(t, nodes, 3*time.Second)
	if leader == nil {
		t.Fatal("no leader")
	}

	// SET foo=bar
	resp, err := leader.Apply("c1", 1, kvCmd("set", "foo", "bar"))
	if err != nil || !resp.Success {
		t.Fatalf("SET: err=%v success=%v", err, resp.Success)
	}

	// SET baz=qux
	resp, err = leader.Apply("c1", 2, kvCmd("set", "baz", "qux"))
	if err != nil || !resp.Success {
		t.Fatalf("SET: err=%v success=%v", err, resp.Success)
	}

	// Wait for followers to apply.
	time.Sleep(100 * time.Millisecond)

	// Verify all nodes have the same state.
	for i, sm := range sms {
		v, ok := sm.get("foo")
		if !ok || v != "bar" {
			t.Errorf("node %d: foo=%q ok=%v, want bar", i, v, ok)
		}
		v, ok = sm.get("baz")
		if !ok || v != "qux" {
			t.Errorf("node %d: baz=%q ok=%v, want qux", i, v, ok)
		}
	}

	// DEL foo
	resp, err = leader.Apply("c1", 3, kvCmd("del", "foo", ""))
	if err != nil || !resp.Success {
		t.Fatalf("DEL: err=%v success=%v", err, resp.Success)
	}

	time.Sleep(100 * time.Millisecond)

	for i, sm := range sms {
		if _, ok := sm.get("foo"); ok {
			t.Errorf("node %d: foo still present after DEL", i)
		}
	}
}

// TestLeaderCrashAndReelection verifies that when a leader is stopped,
// the remaining nodes elect a new leader and continue to accept commands.
func TestLeaderCrashAndReelection(t *testing.T) {
	ids := []types.ServerID{"n1", "n2", "n3"}
	nodes, stop := startCluster(t, ids)
	defer stop()

	leader := waitForLeader(t, nodes, 3*time.Second)
	if leader == nil {
		t.Fatal("no leader")
	}

	// Apply a command before crash.
	resp, err := leader.Apply("c1", 1, []byte("before-crash"))
	if err != nil || !resp.Success {
		t.Fatalf("Apply before crash: err=%v", err)
	}

	// Stop the leader.
	leaderID := leader.cfg.ID
	leader.Stop()

	// Remaining nodes should elect a new leader.
	var remaining []*RaftNode
	for _, n := range nodes {
		if n.cfg.ID != leaderID {
			remaining = append(remaining, n)
		}
	}

	newLeader := waitForLeader(t, remaining, 5*time.Second)
	if newLeader == nil {
		t.Fatal("no new leader elected after crash")
	}
	if newLeader.cfg.ID == leaderID {
		t.Fatal("new leader is the crashed node")
	}

	// Apply a command to the new leader.
	resp, err = newLeader.Apply("c1", 2, []byte("after-crash"))
	if err != nil || !resp.Success {
		t.Fatalf("Apply after crash: err=%v success=%v", err, resp.Success)
	}

	ci := newLeader.state.getCommitIndex()
	if ci < 2 {
		t.Errorf("new leader commitIndex=%d, want >=2", ci)
	}
}

// TestFollowerRejectsApply verifies that a follower returns not-success
// with the leader's ID when a client sends Apply to it.
func TestFollowerRejectsApply(t *testing.T) {
	ids := []types.ServerID{"n1", "n2", "n3"}
	nodes, stop := startCluster(t, ids)
	defer stop()

	leader := waitForLeader(t, nodes, 3*time.Second)
	if leader == nil {
		t.Fatal("no leader")
	}

	// Find a follower.
	var follower *RaftNode
	for _, n := range nodes {
		if n.cfg.ID != leader.cfg.ID {
			follower = n
			break
		}
	}

	resp, err := follower.Apply("c1", 1, []byte("hello"))
	if err != nil {
		t.Fatalf("Apply on follower: %v", err)
	}
	if resp.Success {
		t.Error("follower Apply returned success, want failure")
	}
	if resp.LeaderID == "" {
		t.Error("follower Apply did not return leader ID")
	}
}
