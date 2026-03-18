// Package types defines all shared data types for the raft-lib library.
// These types are used across the raft, transport, and storage packages.
package types

// ServerID identifies a Raft node, typically "host:port".
type ServerID string

// LogType distinguishes the kind of entry stored in the Raft log.
type LogType int

const (
	// LogCommand is a regular application command submitted by a client.
	LogCommand LogType = 0
	// LogConfiguration is a cluster membership change (add/remove server).
	LogConfiguration LogType = 1
	// LogNoop is the no-op entry the leader appends on winning an election.
	LogNoop LogType = 2
)

// ConfigChange carries the data for a membership change log entry.
type ConfigChange struct {
	Add    bool
	Server ServerID
}

// RaftLog is the unit stored in the Raft log and replicated over the wire.
// For LogCommand entries, Data holds the serialized application command.
// For LogConfiguration entries, Config holds the membership change.
type RaftLog struct {
	Index  int
	Term   int
	Type   LogType
	Data   []byte      // application payload (LogCommand)
	Config ConfigChange // membership change (LogConfiguration)
}

// Config holds the startup configuration for a RaftNode.
type Config struct {
	// ID is this node's unique identifier (e.g. "127.0.0.1:8001").
	ID ServerID
	// Peers lists the other nodes in the initial cluster.
	Peers []ServerID
	// HeartbeatMs is the leader heartbeat interval in milliseconds (default 20).
	HeartbeatMs int
	// ElectionMinMs is the minimum election timeout in milliseconds (default 150).
	ElectionMinMs int
	// ElectionMaxMs is the maximum election timeout in milliseconds (default 300).
	ElectionMaxMs int
}

// DefaultConfig returns a Config with sensible defaults.
func DefaultConfig(id ServerID, peers []ServerID) Config {
	return Config{
		ID:            id,
		Peers:         peers,
		HeartbeatMs:   20,
		ElectionMinMs: 150,
		ElectionMaxMs: 300,
	}
}

// PersistentState is the Raft state that must survive crashes.
// It is saved to stable storage before responding to RPCs.
type PersistentState struct {
	CurrentTerm int
	VotedFor    ServerID
	Logs        []RaftLog
}

// Snapshot represents a compacted prefix of the Raft log.
type Snapshot struct {
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
	ServerConfig      map[ServerID]bool
	Hash              [32]byte
}

// ---- RPC wire types ------------------------------------------------

// AppendEntriesArgs is the argument for the AppendEntries RPC.
// Used for both heartbeats (Entries == nil) and log replication.
type AppendEntriesArgs struct {
	Term         int
	LeaderID     ServerID
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []RaftLog
	LeaderCommit int
}

// AppendEntriesResponse is the reply to an AppendEntries RPC.
type AppendEntriesResponse struct {
	ID        ServerID
	Term      int
	Success   bool
	LastIndex int
}

// RequestVoteArgs is the argument for the RequestVote RPC.
type RequestVoteArgs struct {
	Term        int
	CandidateID ServerID
	LastLogIndex int
	LastLogTerm  int
}

// RequestVoteResponse is the reply to a RequestVote RPC.
type RequestVoteResponse struct {
	ID          ServerID
	Term        int
	VoteGranted bool
}

// InstallSnapshotArgs is the argument for the InstallSnapshot RPC.
type InstallSnapshotArgs struct {
	ID                  ServerID
	Term                int
	LastIncludedIndex   int
	LastIncludedTerm    int
	Data                []byte
	ServerConfiguration map[ServerID]bool
	Hash                [32]byte
}

// InstallSnapshotResponse is the reply to an InstallSnapshot RPC.
type InstallSnapshotResponse struct {
	ID                ServerID
	Term              int
	Success           bool
	LastIncludedIndex int
	LastIncludedTerm  int
}

// ---- Client-facing types -------------------------------------------

// ApplyArgs is used by clients to submit commands to the cluster.
type ApplyArgs struct {
	// ClientID identifies the client (used for deduplication).
	ClientID string
	// SeqNum is a monotonically increasing sequence number per client.
	SeqNum int64
	// Command is the opaque application command to be applied.
	Command []byte
}

// ApplyResponse is returned after a command is committed to the log.
type ApplyResponse struct {
	Success  bool
	LeaderID ServerID
}

// AddRemoveServerArgs is used to request a membership change.
type AddRemoveServerArgs struct {
	Server ServerID
	Add    bool
}

// AddRemoveServerResponse is returned after a membership change is committed.
type AddRemoveServerResponse struct {
	Success  bool
	LeaderID ServerID
}

// ---- Core interfaces -----------------------------------------------

// StateMachine is the interface that applications must implement.
// The RaftNode calls Apply for every committed log entry in order.
// Snapshot and Restore are used for log compaction.
type StateMachine interface {
	// Apply executes a committed command against the state machine.
	Apply(command []byte) error
	// Snapshot captures the current state machine state.
	Snapshot() ([]byte, error)
	// Restore replaces the state machine state from a snapshot.
	Restore(snapshot []byte) error
}
