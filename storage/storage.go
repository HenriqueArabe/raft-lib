// Package storage defines the persistence abstraction for Raft nodes
// and provides an in-memory implementation suitable for testing.
package storage

import (
	"github.com/henrique-arab/raft-lib/types"
)

// Storage abstracts the stable storage layer used by the Raft algorithm.
// Implementations must persist data durably before returning from Save calls.
// Implementations must be safe for concurrent use.
type Storage interface {
	// SaveState persists the Raft persistent state (term, votedFor, log).
	// Must be called before responding to RPCs that update these fields.
	SaveState(state *types.PersistentState) error

	// LoadState retrieves the last persisted Raft state.
	// Returns an empty PersistentState (not an error) if none exists yet.
	LoadState() (*types.PersistentState, error)

	// SaveSnapshot persists a compacted snapshot.
	SaveSnapshot(snapshot *types.Snapshot) error

	// LoadSnapshot retrieves the last persisted snapshot.
	// Returns nil (not an error) if no snapshot exists yet.
	LoadSnapshot() (*types.Snapshot, error)
}
