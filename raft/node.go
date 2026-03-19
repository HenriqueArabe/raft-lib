// Package raft provides a modular, reusable implementation of the Raft
// consensus algorithm. Applications integrate by implementing the
// types.StateMachine interface and supplying a transport.Transport and
// storage.Storage implementation to New().
package raft

import (
	"fmt"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/henrique-arab/raft-lib/storage"
	"github.com/henrique-arab/raft-lib/transport"
	"github.com/henrique-arab/raft-lib/types"
)

// RaftNode is the main entry point of the library.
// Create one with New(), then call Start(). Submit commands with Apply().
type RaftNode struct {
	cfg types.Config

	// Pluggable components (injected at construction time).
	sm        types.StateMachine
	transport transport.Transport
	storage   storage.Storage

	// Internal Raft state.
	state *nodeState

	// ---- Inbound RPC channels (filled by HandleXxx methods) ----
	// AppendEntries RPC pipeline.
	aeArgsChan   chan *types.AppendEntriesArgs
	aeReplyChan  chan *types.AppendEntriesResponse
	// Responses that come back from peers we sent AppendEntries to.
	myAEResponseChan chan *types.AppendEntriesResponse

	// RequestVote RPC pipeline.
	rvArgsChan  chan *types.RequestVoteArgs
	rvReplyChan chan *types.RequestVoteResponse
	// Responses from peers we sent RequestVote to.
	myVoteResponseChan chan *types.RequestVoteResponse

	// InstallSnapshot RPC pipeline.
	isArgsChan  chan *types.InstallSnapshotArgs
	isReplyChan chan *types.InstallSnapshotResponse
	// Responses from peers we sent InstallSnapshot to.
	myISResponseChan chan *types.InstallSnapshotResponse

	// ---- Client-facing channels ----
	// applyChan receives commands submitted via Apply().
	applyChan chan pendingApply
	// confChan receives membership change requests.
	confChan chan configEntry

	// ---- Snapshot channels (shared with nodeState) ----
	snapshotRequestChan  chan struct{}
	snapshotResponseChan chan []byte
	snapshotInstallChan  chan []byte

	// ---- Lifecycle ----
	// connectedChan is closed (or receives) when all peer connections are up.
	connectedChan chan struct{}
	connected     bool
	stopCh        chan struct{}
	once          sync.Once // for Stop()

	// ---- Pending command tracking (leader) ----
	pendingMu      sync.Mutex
	pendingCommands map[int]chan error // logIndex -> result channel
}

// pendingApply is an in-flight client command waiting for a response.
type pendingApply struct {
	args     types.ApplyArgs
	resultCh chan *types.ApplyResponse
}

// New creates a RaftNode with the given configuration and pluggable components.
// The node is not started yet; call Start() after New().
func New(
	cfg types.Config,
	sm types.StateMachine,
	tr transport.Transport,
	st storage.Storage,
) (*RaftNode, error) {
	if cfg.HeartbeatMs == 0 {
		cfg.HeartbeatMs = 20
	}
	if cfg.ElectionMinMs == 0 {
		cfg.ElectionMinMs = 150
	}
	if cfg.ElectionMaxMs == 0 {
		cfg.ElectionMaxMs = 300
	}
	if sm == nil {
		return nil, fmt.Errorf("raft.New: StateMachine must not be nil")
	}
	if tr == nil {
		return nil, fmt.Errorf("raft.New: Transport must not be nil")
	}
	if st == nil {
		return nil, fmt.Errorf("raft.New: Storage must not be nil")
	}

	snapshotReqChan := make(chan struct{}, 1)
	snapshotRespChan := make(chan []byte, 1)
	snapshotInstallChan := make(chan []byte, 1)

	n := &RaftNode{
		cfg:       cfg,
		sm:        sm,
		transport: tr,
		storage:   st,

		state: newNodeState(
			cfg.ID,
			cfg.Peers,
			cfg.ElectionMinMs,
			cfg.ElectionMaxMs,
			snapshotReqChan,
			snapshotRespChan,
			snapshotInstallChan,
		),

		aeArgsChan:        make(chan *types.AppendEntriesArgs),
		aeReplyChan:       make(chan *types.AppendEntriesResponse),
		myAEResponseChan:  make(chan *types.AppendEntriesResponse),

		rvArgsChan:         make(chan *types.RequestVoteArgs),
		rvReplyChan:        make(chan *types.RequestVoteResponse),
		myVoteResponseChan: make(chan *types.RequestVoteResponse),

		isArgsChan:       make(chan *types.InstallSnapshotArgs),
		isReplyChan:      make(chan *types.InstallSnapshotResponse),
		myISResponseChan: make(chan *types.InstallSnapshotResponse),

		applyChan: make(chan pendingApply),
		confChan:  make(chan configEntry),

		snapshotRequestChan:  snapshotReqChan,
		snapshotResponseChan: snapshotRespChan,
		snapshotInstallChan:  snapshotInstallChan,

		connectedChan:   make(chan struct{}, 1),
		stopCh:          make(chan struct{}),
		pendingCommands: make(map[int]chan error),
	}

	return n, nil
}

// Start initialises storage/transport and launches the Raft goroutines.
// TODO: Session 5 — load persistent state, connect to peers.
func (n *RaftNode) Start() error {
	// Load persistent state from storage.
	ps, err := n.storage.LoadState()
	if err != nil {
		return fmt.Errorf("Start: load state: %w", err)
	}
	if ps != nil && ps.CurrentTerm > 0 {
		n.state.mu.Lock()
		n.state.currentTerm = ps.CurrentTerm
		n.state.votedFor = ps.VotedFor
		// Restore log entries.
		for i, rl := range ps.Logs {
			if i >= logArrayCapacity {
				break
			}
			n.state.logs[i] = rl
			n.state.nextLogIdx = i + 1
		}
		n.state.mu.Unlock()
	}

	// Restore snapshot if available.
	snap, err := n.storage.LoadSnapshot()
	if err != nil {
		return fmt.Errorf("Start: load snapshot: %w", err)
	}
	if snap != nil {
		n.state.mu.Lock()
		n.state.lastSnapshot = snap
		n.state.commitIndex = snap.LastIncludedIndex
		n.state.lastApplied = snap.LastIncludedIndex
		n.state.mu.Unlock()
		if err := n.sm.Restore(snap.Data); err != nil {
			return fmt.Errorf("Start: restore snapshot: %w", err)
		}
	}

	// Register ourselves as the transport handler.
	if rpcTr, ok := n.transport.(interface {
		RegisterHandler(transport.TransportHandler)
	}); ok {
		rpcTr.RegisterHandler(n)
	}

	// Start the RPC server.
	if err := n.transport.Listen(string(n.cfg.ID)); err != nil {
		return fmt.Errorf("Start: listen: %w", err)
	}

	// Connect to peers in the background; signal connectedChan when all are up.
	if len(n.cfg.Peers) == 0 {
		n.connected = true
	} else {
		go n.connectToPeers()
	}

	// Launch background goroutines.
	go n.handleApplyRequests()
	go n.handleConfigurationMessages()
	go n.handleAppendEntriesRPCResponses()
	go n.handleInstallSnapshotResponses()
	go n.snapshotLoop()
	go n.run()

	log.Infof("RaftNode %s started (peers=%v)", n.cfg.ID, n.cfg.Peers)
	return nil
}

// Stop gracefully shuts down the node.
func (n *RaftNode) Stop() {
	n.once.Do(func() {
		close(n.stopCh)
		n.transport.Close()
		log.Infof("RaftNode %s stopped", n.cfg.ID)
	})
}

// Apply submits a command to the Raft cluster.
// Blocks until the command is committed and applied (or the node is stopped).
// Returns an error if this node is not the leader (includes the leader's address).
func (n *RaftNode) Apply(clientID string, seqNum int64, command []byte) (*types.ApplyResponse, error) {
	resultCh := make(chan *types.ApplyResponse, 1)
	req := pendingApply{
		args: types.ApplyArgs{
			ClientID: clientID,
			SeqNum:   seqNum,
			Command:  command,
		},
		resultCh: resultCh,
	}
	select {
	case n.applyChan <- req:
	case <-n.stopCh:
		return nil, fmt.Errorf("node stopped")
	}
	select {
	case resp := <-resultCh:
		return resp, nil
	case <-n.stopCh:
		return nil, fmt.Errorf("node stopped")
	}
}

// AddServer requests that server be added to the cluster.
// Must be called on the current leader.
func (n *RaftNode) AddServer(server types.ServerID) (*types.AddRemoveServerResponse, error) {
	return n.membershipChange(types.ConfigChange{Add: true, Server: server})
}

// RemoveServer requests that server be removed from the cluster.
// Must be called on the current leader.
func (n *RaftNode) RemoveServer(server types.ServerID) (*types.AddRemoveServerResponse, error) {
	return n.membershipChange(types.ConfigChange{Add: false, Server: server})
}

func (n *RaftNode) membershipChange(cfg types.ConfigChange) (*types.AddRemoveServerResponse, error) {
	resultCh := make(chan *types.AddRemoveServerResponse, 1)
	entry := configEntry{
		msg:         cfg,
		chanApplied: make(chan bool, 1),
		resultCh:    resultCh,
	}
	select {
	case n.confChan <- entry:
	case <-n.stopCh:
		return nil, fmt.Errorf("node stopped")
	}
	select {
	case resp := <-resultCh:
		return resp, nil
	case <-n.stopCh:
		return nil, fmt.Errorf("node stopped")
	}
}

// ---- Transport handler (incoming RPC dispatch) --------------------

// HandleRequestVote is called by the transport when a peer sends a RequestVote RPC.
// It puts the args into the channel and waits for the response from the run loop.
func (n *RaftNode) HandleRequestVote(args *types.RequestVoteArgs) *types.RequestVoteResponse {
	n.rvArgsChan <- args
	return <-n.rvReplyChan
}

// HandleAppendEntries is called by the transport when a peer sends AppendEntries.
func (n *RaftNode) HandleAppendEntries(args *types.AppendEntriesArgs) *types.AppendEntriesResponse {
	n.aeArgsChan <- args
	return <-n.aeReplyChan
}

// HandleInstallSnapshot is called by the transport when a peer sends InstallSnapshot.
func (n *RaftNode) HandleInstallSnapshot(args *types.InstallSnapshotArgs) *types.InstallSnapshotResponse {
	n.isArgsChan <- args
	return <-n.isReplyChan
}

// HandleApply is called by the transport when a client forwards a command.
func (n *RaftNode) HandleApply(args *types.ApplyArgs) *types.ApplyResponse {
	resp, err := n.Apply(args.ClientID, args.SeqNum, args.Command)
	if err != nil {
		return &types.ApplyResponse{Success: false, LeaderID: n.state.getCurrentLeader()}
	}
	return resp
}

// HandleAddRemoveServer is called by the transport for membership change requests.
func (n *RaftNode) HandleAddRemoveServer(args *types.AddRemoveServerArgs) *types.AddRemoveServerResponse {
	var resp *types.AddRemoveServerResponse
	var err error
	if args.Add {
		resp, err = n.AddServer(args.Server)
	} else {
		resp, err = n.RemoveServer(args.Server)
	}
	if err != nil {
		return &types.AddRemoveServerResponse{Success: false, LeaderID: n.state.getCurrentLeader()}
	}
	return resp
}

// ---- Main run loop ------------------------------------------------

// run is the central Raft loop. It switches behaviour based on the node's role.
func (n *RaftNode) run() {
	for {
		select {
		case <-n.stopCh:
			return
		default:
		}

		n.checkLogsToApply()
		n.checkConfigurationsToStart()

		switch n.state.getRole() {
		case Follower:
			n.handleFollower()
		case Candidate:
			n.handleCandidate()
		case Leader:
			n.handleLeader()
		}
	}
}

// checkConfigurationsToStart tries to start the next queued config change.
func (n *RaftNode) checkConfigurationsToStart() {
	ok, entry := n.state.handleNextConfigurationChange()
	if ok {
		go n.waitForConfigApplied(entry)
	}
}

// handleApplyRequests is the goroutine that serialises client command submissions.
// TODO: Session 3 — implement deduplication using clientLastSeq.
func (n *RaftNode) handleApplyRequests() {
	for {
		select {
		case req := <-n.applyChan:
			switch n.state.getRole() {
			case Follower, Candidate:
				req.resultCh <- &types.ApplyResponse{
					Success:  false,
					LeaderID: n.state.getCurrentLeader(),
				}
			case Leader:
				logIdx := n.state.addCommandLog(n.cfg.ID, req.args.Command)
				if logIdx < 0 {
					req.resultCh <- &types.ApplyResponse{Success: false, LeaderID: n.cfg.ID}
					continue
				}
				// Register result channel for when the log is applied.
				errCh := make(chan error, 1)
				n.pendingMu.Lock()
				n.pendingCommands[logIdx] = errCh
				n.pendingMu.Unlock()

				n.sendAppendEntriesRPCs()

				go func(resultCh chan *types.ApplyResponse, errCh chan error) {
					err := <-errCh
					success := err == nil
					resultCh <- &types.ApplyResponse{Success: success, LeaderID: n.cfg.ID}
				}(req.resultCh, errCh)
			}
		case <-n.stopCh:
			return
		}
	}
}

// handleFollower handles one iteration of the Follower select loop.
func (n *RaftNode) handleFollower() {
	timer := n.state.checkElectionTimeout()

	select {
	case args := <-n.aeArgsChan:
		n.state.stopElectionTimeout()
		resp := n.state.handleAppendEntries(n.cfg.ID, args)
		n.aeReplyChan <- resp

	case args := <-n.rvArgsChan:
		n.state.stopElectionTimeout()
		n.rvReplyChan <- n.state.handleRequestToVote(n.cfg.ID, args)

	case args := <-n.isArgsChan:
		resp := n.state.handleInstallSnapshotRequest(n.cfg.ID, args)
		n.isReplyChan <- resp

	case <-n.connectedChan:
		n.connected = true

	case <-timer.C:
		n.state.stopElectionTimeout()
		if !n.connected {
			return
		}
		n.state.startElection(n.cfg.ID)
		if len(n.cfg.Peers) == 0 {
			n.state.winElection(n.cfg.ID)
		} else {
			args := n.state.prepareRequestVoteRPC(n.cfg.ID)
			n.sendRequestVoteRPCs(args)
		}

	// Drain stale vote responses.
	case <-n.myVoteResponseChan:

	case <-n.stopCh:
	}
}

// handleCandidate handles one iteration of the Candidate select loop.
func (n *RaftNode) handleCandidate() {
	timer := n.state.checkElectionTimeout()

	select {
	case args := <-n.aeArgsChan:
		resp := n.state.handleAppendEntries(n.cfg.ID, args)
		n.aeReplyChan <- resp

	case args := <-n.rvArgsChan:
		n.rvReplyChan <- n.state.handleRequestToVote(n.cfg.ID, args)

	case args := <-n.isArgsChan:
		resp := n.state.handleInstallSnapshotRequest(n.cfg.ID, args)
		n.isReplyChan <- resp

	case resp := <-n.myVoteResponseChan:
		if won := n.state.updateElection(resp); won {
			n.state.winElection(n.cfg.ID)
			n.sendAppendEntriesRPCs()
		}

	case <-timer.C:
		n.state.stopElectionTimeout()
		n.state.startElection(n.cfg.ID)
		args := n.state.prepareRequestVoteRPC(n.cfg.ID)
		n.sendRequestVoteRPCs(args)

	case <-n.connectedChan:
		n.connected = true

	case <-n.stopCh:
	}
}

// handleLeader handles one iteration of the Leader select loop.
func (n *RaftNode) handleLeader() {
	select {
	case args := <-n.aeArgsChan:
		resp := n.state.handleAppendEntries(n.cfg.ID, args)
		n.aeReplyChan <- resp

	case args := <-n.rvArgsChan:
		n.rvReplyChan <- n.state.handleRequestToVote(n.cfg.ID, args)

	case <-n.myVoteResponseChan: // drain

	case args := <-n.isArgsChan:
		resp := n.state.handleInstallSnapshotRequest(n.cfg.ID, args)
		n.isReplyChan <- resp

	case <-n.connectedChan:
		n.connected = true

	case <-time.After(time.Duration(n.cfg.HeartbeatMs) * time.Millisecond):
		n.sendAppendEntriesRPCs()

	case <-n.stopCh:
		return
	}

	n.state.checkCommits()
}

// connectToPeers connects to every peer in parallel (with retry) and signals
// connectedChan once all connections are established.
func (n *RaftNode) connectToPeers() {
	done := make(chan struct{}, len(n.cfg.Peers))
	for _, peer := range n.cfg.Peers {
		peer := peer
		go func() {
			for {
				select {
				case <-n.stopCh:
					return
				default:
				}
				if err := n.transport.Connect(peer); err != nil {
					log.Debugf("Connect to %s: %v — retrying", peer, err)
					time.Sleep(time.Second)
					continue
				}
				log.Infof("Connected to peer %s", peer)
				done <- struct{}{}
				return
			}
		}()
	}
	for i := 0; i < len(n.cfg.Peers); i++ {
		select {
		case <-done:
		case <-n.stopCh:
			return
		}
	}
	select {
	case n.connectedChan <- struct{}{}:
	case <-n.stopCh:
	}
}

// Ensure RaftNode implements transport.TransportHandler at compile time.
var _ transport.TransportHandler = (*RaftNode)(nil)
