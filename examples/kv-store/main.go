// kv-store is a minimal distributed key-value store built on top of raft-lib.
// It demonstrates how to implement the types.StateMachine interface and wire
// up a Raft cluster with RPCTransport.
//
// Usage:
//
//	# Terminal 1
//	go run . -id 127.0.0.1:9001 -peers 127.0.0.1:9002,127.0.0.1:9003
//
//	# Terminal 2
//	go run . -id 127.0.0.1:9002 -peers 127.0.0.1:9001,127.0.0.1:9003
//
//	# Terminal 3
//	go run . -id 127.0.0.1:9003 -peers 127.0.0.1:9001,127.0.0.1:9002
//
// Once a leader is elected, type commands on the leader's terminal:
//
//	SET mykey myvalue
//	GET mykey
//	DEL mykey
package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strings"
	"sync"

	log "github.com/sirupsen/logrus"

	"github.com/henrique-arab/raft-lib/raft"
	"github.com/henrique-arab/raft-lib/storage"
	"github.com/henrique-arab/raft-lib/transport"
	"github.com/henrique-arab/raft-lib/types"
)

// ---- KV State Machine ---------------------------------------------

// Op represents a single key-value operation.
type Op struct {
	Kind  string `json:"kind"`  // "set" or "del"
	Key   string `json:"key"`
	Value string `json:"value,omitempty"`
}

// KVSM is a key-value StateMachine backed by an in-memory map.
type KVSM struct {
	mu   sync.RWMutex
	data map[string]string
}

func NewKVSM() *KVSM {
	return &KVSM{data: make(map[string]string)}
}

func (kv *KVSM) Apply(command []byte) error {
	var op Op
	if err := json.Unmarshal(command, &op); err != nil {
		return fmt.Errorf("unmarshal op: %w", err)
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

func (kv *KVSM) Snapshot() ([]byte, error) {
	kv.mu.RLock()
	defer kv.mu.RUnlock()
	return json.Marshal(kv.data)
}

func (kv *KVSM) Restore(snapshot []byte) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	kv.data = make(map[string]string)
	return json.Unmarshal(snapshot, &kv.data)
}

func (kv *KVSM) Get(key string) (string, bool) {
	kv.mu.RLock()
	defer kv.mu.RUnlock()
	v, ok := kv.data[key]
	return v, ok
}

// ---- main ----------------------------------------------------------

func main() {
	id := flag.String("id", "", "this node's address (e.g. 127.0.0.1:9001)")
	peersStr := flag.String("peers", "", "comma-separated peer addresses")
	flag.Parse()

	if *id == "" {
		fmt.Fprintln(os.Stderr, "usage: kv-store -id <addr> -peers <addr1,addr2,...>")
		os.Exit(1)
	}

	log.SetLevel(log.InfoLevel)

	var peers []types.ServerID
	if *peersStr != "" {
		for _, p := range strings.Split(*peersStr, ",") {
			peers = append(peers, types.ServerID(strings.TrimSpace(p)))
		}
	}

	cfg := types.Config{
		ID:            types.ServerID(*id),
		Peers:         peers,
		HeartbeatMs:   50,
		ElectionMinMs: 300,
		ElectionMaxMs: 500,
	}

	sm := NewKVSM()
	tr := transport.NewRPCTransport(cfg.ID)
	st := storage.NewMemoryStorage()

	node, err := raft.New(cfg, sm, tr, st)
	if err != nil {
		log.Fatalf("raft.New: %v", err)
	}
	if err := node.Start(); err != nil {
		log.Fatalf("raft.Start: %v", err)
	}
	defer node.Stop()

	fmt.Printf("Node %s started. Peers: %v\n", *id, peers)
	fmt.Println("Commands: SET <key> <value> | GET <key> | DEL <key>")
	fmt.Println("Waiting for leader election...")

	var seqNum int64
	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		parts := strings.Fields(line)
		cmd := strings.ToUpper(parts[0])

		switch cmd {
		case "GET":
			if len(parts) < 2 {
				fmt.Println("usage: GET <key>")
				continue
			}
			v, ok := sm.Get(parts[1])
			if ok {
				fmt.Printf("%s = %s\n", parts[1], v)
			} else {
				fmt.Printf("%s: not found\n", parts[1])
			}

		case "SET":
			if len(parts) < 3 {
				fmt.Println("usage: SET <key> <value>")
				continue
			}
			op := Op{Kind: "set", Key: parts[1], Value: strings.Join(parts[2:], " ")}
			data, _ := json.Marshal(op)
			seqNum++
			resp, err := node.Apply(*id, seqNum, data)
			if err != nil {
				fmt.Printf("error: %v\n", err)
			} else if !resp.Success {
				fmt.Printf("not leader (leader=%s)\n", resp.LeaderID)
			} else {
				fmt.Println("OK")
			}

		case "DEL":
			if len(parts) < 2 {
				fmt.Println("usage: DEL <key>")
				continue
			}
			op := Op{Kind: "del", Key: parts[1]}
			data, _ := json.Marshal(op)
			seqNum++
			resp, err := node.Apply(*id, seqNum, data)
			if err != nil {
				fmt.Printf("error: %v\n", err)
			} else if !resp.Success {
				fmt.Printf("not leader (leader=%s)\n", resp.LeaderID)
			} else {
				fmt.Println("OK")
			}

		default:
			fmt.Println("unknown command:", cmd)
		}
	}

}
