package transport

import (
	"fmt"
	"net"
	"net/http"
	"net/rpc"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/henrique-arab/raft-lib/types"
)

const (
	defaultDialTimeout    = 300 * time.Second
	defaultRPCTimeout     = 200 * time.Millisecond
	defaultRetryInterval  = 1 * time.Second
)

// rpcConn wraps a single *rpc.Client connection to a peer.
type rpcConn struct {
	client *rpc.Client
}

// RPCTransport implements Transport using Go's standard net/rpc package.
// RPCs are sent over HTTP/TCP to peer nodes.
type RPCTransport struct {
	mu      sync.RWMutex
	myID    types.ServerID
	handler TransportHandler

	// connections holds established rpcConn values keyed by ServerID.
	connections sync.Map

	listener net.Listener
}

// NewRPCTransport creates an RPCTransport for the given node ID.
// Call Listen() to start accepting inbound RPCs, then Connect() for each peer.
func NewRPCTransport(id types.ServerID) *RPCTransport {
	return &RPCTransport{myID: id}
}

// RegisterHandler sets the callback target for incoming RPCs.
// Must be called before Listen().
func (t *RPCTransport) RegisterHandler(h TransportHandler) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.handler = h
}

// Listen starts the RPC server on addr (e.g. "127.0.0.1:8001").
func (t *RPCTransport) Listen(addr string) error {
	listener := &rpcListener{transport: t}

	srv := rpc.NewServer()
	if err := srv.Register(listener); err != nil {
		return fmt.Errorf("rpc register: %w", err)
	}
	srv.HandleHTTP(rpc.DefaultRPCPath+string(t.myID), rpc.DefaultDebugPath+string(t.myID))

	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("listen %s: %w", addr, err)
	}
	t.listener = ln
	log.Infof("RPCTransport: listening on %s", addr)
	go http.Serve(ln, nil)
	return nil
}

// Connect establishes or re-establishes a connection to a peer.
func (t *RPCTransport) Connect(id types.ServerID) error {
	// Close any existing connection first.
	t.Disconnect(id)

	client, err := rpc.DialHTTP("tcp", string(id))
	if err != nil {
		return fmt.Errorf("dial %s: %w", id, err)
	}
	t.connections.Store(id, rpcConn{client: client})
	log.Infof("RPCTransport: connected to %s", id)
	return nil
}

// ConnectWithRetry tries to connect to a peer, retrying until success.
// Runs in a goroutine; reports completion via done channel.
func (t *RPCTransport) ConnectWithRetry(id types.ServerID, done chan<- types.ServerID) {
	go func() {
		for {
			if err := t.Connect(id); err != nil {
				log.Debugf("RPCTransport: retry connect to %s: %v", id, err)
				time.Sleep(defaultRetryInterval)
				continue
			}
			done <- id
			return
		}
	}()
}

// Disconnect closes the connection to a peer and removes it from the map.
func (t *RPCTransport) Disconnect(id types.ServerID) error {
	if val, loaded := t.connections.LoadAndDelete(id); loaded {
		conn := val.(rpcConn)
		if conn.client != nil {
			return conn.client.Close()
		}
	}
	return nil
}

// Close shuts down the listener and all peer connections.
func (t *RPCTransport) Close() error {
	if t.listener != nil {
		t.listener.Close()
	}
	t.connections.Range(func(k, v interface{}) bool {
		conn := v.(rpcConn)
		if conn.client != nil {
			conn.client.Close()
		}
		t.connections.Delete(k)
		return true
	})
	return nil
}

// SendRequestVote sends a RequestVote RPC to target. Blocking with timeout.
func (t *RPCTransport) SendRequestVote(target types.ServerID, args *types.RequestVoteArgs) (*types.RequestVoteResponse, error) {
	var reply types.RequestVoteResponse
	err := t.call(target, "RaftRPCListener.RequestVoteRPC", args, &reply, defaultRPCTimeout)
	if err != nil {
		return nil, err
	}
	return &reply, nil
}

// SendAppendEntries sends an AppendEntries RPC to target. Blocking with timeout.
func (t *RPCTransport) SendAppendEntries(target types.ServerID, args *types.AppendEntriesArgs) (*types.AppendEntriesResponse, error) {
	var reply types.AppendEntriesResponse
	err := t.call(target, "RaftRPCListener.AppendEntriesRPC", args, &reply, defaultRPCTimeout)
	if err != nil {
		return nil, err
	}
	return &reply, nil
}

// SendInstallSnapshot sends an InstallSnapshot RPC to target. Blocking with timeout.
func (t *RPCTransport) SendInstallSnapshot(target types.ServerID, args *types.InstallSnapshotArgs) (*types.InstallSnapshotResponse, error) {
	var reply types.InstallSnapshotResponse
	err := t.call(target, "RaftRPCListener.InstallSnapshotRPC", args, &reply, defaultRPCTimeout)
	if err != nil {
		return nil, err
	}
	return &reply, nil
}

// SendApply forwards a client command to the target (typically the leader).
func (t *RPCTransport) SendApply(target types.ServerID, args *types.ApplyArgs) (*types.ApplyResponse, error) {
	var reply types.ApplyResponse
	err := t.call(target, "RaftRPCListener.ApplyRPC", args, &reply, 500*time.Millisecond)
	if err != nil {
		return nil, err
	}
	return &reply, nil
}

// SendAddRemoveServer sends a membership change request to the target.
func (t *RPCTransport) SendAddRemoveServer(target types.ServerID, args *types.AddRemoveServerArgs) (*types.AddRemoveServerResponse, error) {
	var reply types.AddRemoveServerResponse
	err := t.call(target, "RaftRPCListener.AddRemoveServerRPC", args, &reply, 500*time.Millisecond)
	if err != nil {
		return nil, err
	}
	return &reply, nil
}

// call performs an asynchronous RPC with a deadline, reconnecting on failure.
func (t *RPCTransport) call(target types.ServerID, method string, args, reply interface{}, timeout time.Duration) error {
	val, ok := t.connections.Load(target)
	if !ok {
		return fmt.Errorf("no connection to %s", target)
	}
	conn := val.(rpcConn)
	if conn.client == nil {
		return fmt.Errorf("nil connection to %s", target)
	}

	call := conn.client.Go(method, args, reply, nil)
	select {
	case <-call.Done:
		if call.Error != nil {
			// Mark connection as stale so the caller can reconnect.
			t.connections.Delete(target)
			return call.Error
		}
		return nil
	case <-time.After(timeout):
		return fmt.Errorf("RPC %s to %s timed out", method, target)
	}
}

// ---- inbound RPC listener ------------------------------------------

// rpcListener is the object registered with net/rpc to receive inbound RPCs.
// It dispatches calls to the TransportHandler (i.e., the RaftNode).
type rpcListener struct {
	transport *RPCTransport
}

func (l *rpcListener) handler() TransportHandler {
	l.transport.mu.RLock()
	defer l.transport.mu.RUnlock()
	return l.transport.handler
}

func (l *rpcListener) RequestVoteRPC(args *types.RequestVoteArgs, reply *types.RequestVoteResponse) error {
	resp := l.handler().HandleRequestVote(args)
	*reply = *resp
	return nil
}

func (l *rpcListener) AppendEntriesRPC(args *types.AppendEntriesArgs, reply *types.AppendEntriesResponse) error {
	resp := l.handler().HandleAppendEntries(args)
	*reply = *resp
	return nil
}

func (l *rpcListener) InstallSnapshotRPC(args *types.InstallSnapshotArgs, reply *types.InstallSnapshotResponse) error {
	resp := l.handler().HandleInstallSnapshot(args)
	*reply = *resp
	return nil
}

func (l *rpcListener) ApplyRPC(args *types.ApplyArgs, reply *types.ApplyResponse) error {
	resp := l.handler().HandleApply(args)
	*reply = *resp
	return nil
}

func (l *rpcListener) AddRemoveServerRPC(args *types.AddRemoveServerArgs, reply *types.AddRemoveServerResponse) error {
	resp := l.handler().HandleAddRemoveServer(args)
	*reply = *resp
	return nil
}

// Ensure RPCTransport implements the Transport interface at compile time.
var _ Transport = (*RPCTransport)(nil)
