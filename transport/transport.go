// Package transport defines the inter-node communication abstractions
// and provides a concrete net/rpc-based implementation.
package transport

import (
	"github.com/henrique-arab/raft-lib/types"
)

// Transport abstracts the network layer used by the Raft algorithm.
// Implementations must be safe for concurrent use.
type Transport interface {
	// SendRequestVote sends a RequestVote RPC to the target node.
	SendRequestVote(target types.ServerID, args *types.RequestVoteArgs) (*types.RequestVoteResponse, error)

	// SendAppendEntries sends an AppendEntries RPC to the target node.
	SendAppendEntries(target types.ServerID, args *types.AppendEntriesArgs) (*types.AppendEntriesResponse, error)

	// SendInstallSnapshot sends an InstallSnapshot RPC to the target node.
	SendInstallSnapshot(target types.ServerID, args *types.InstallSnapshotArgs) (*types.InstallSnapshotResponse, error)

	// SendApply forwards a client command to the current leader.
	SendApply(target types.ServerID, args *types.ApplyArgs) (*types.ApplyResponse, error)

	// SendAddRemoveServer sends a membership change request to the leader.
	SendAddRemoveServer(target types.ServerID, args *types.AddRemoveServerArgs) (*types.AddRemoveServerResponse, error)

	// Listen starts the RPC server on the given address.
	Listen(addr string) error

	// Connect establishes or re-establishes a connection to a peer.
	Connect(id types.ServerID) error

	// Disconnect closes the connection to a peer.
	Disconnect(id types.ServerID) error

	// Close shuts down the transport and all open connections.
	Close() error
}

// TransportHandler is implemented by the RaftNode to handle incoming RPCs.
// The transport layer calls these methods when it receives an inbound RPC.
type TransportHandler interface {
	// HandleRequestVote processes an incoming RequestVote RPC.
	HandleRequestVote(args *types.RequestVoteArgs) *types.RequestVoteResponse

	// HandleAppendEntries processes an incoming AppendEntries RPC.
	HandleAppendEntries(args *types.AppendEntriesArgs) *types.AppendEntriesResponse

	// HandleInstallSnapshot processes an incoming InstallSnapshot RPC.
	HandleInstallSnapshot(args *types.InstallSnapshotArgs) *types.InstallSnapshotResponse

	// HandleApply processes an incoming client command RPC.
	HandleApply(args *types.ApplyArgs) *types.ApplyResponse

	// HandleAddRemoveServer processes an incoming membership change RPC.
	HandleAddRemoveServer(args *types.AddRemoveServerArgs) *types.AddRemoveServerResponse
}
