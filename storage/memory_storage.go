package storage

import (
	"sync"

	"github.com/henrique-arab/raft-lib/types"
)

// MemoryStorage is a non-durable, in-memory Storage implementation.
// It is intended for testing and examples only — data is lost on restart.
type MemoryStorage struct {
	mu       sync.RWMutex
	state    *types.PersistentState
	snapshot *types.Snapshot
}

// NewMemoryStorage returns an initialized MemoryStorage.
func NewMemoryStorage() *MemoryStorage {
	return &MemoryStorage{}
}

// SaveState stores the persistent state in memory.
func (m *MemoryStorage) SaveState(state *types.PersistentState) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Deep-copy to prevent the caller from mutating stored state.
	copied := *state
	logs := make([]types.RaftLog, len(state.Logs))
	copy(logs, state.Logs)
	copied.Logs = logs

	m.state = &copied
	return nil
}

// LoadState retrieves the in-memory persistent state.
func (m *MemoryStorage) LoadState() (*types.PersistentState, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.state == nil {
		return &types.PersistentState{}, nil
	}

	// Return a copy so callers cannot mutate internal state.
	copied := *m.state
	logs := make([]types.RaftLog, len(m.state.Logs))
	copy(logs, m.state.Logs)
	copied.Logs = logs
	return &copied, nil
}

// SaveSnapshot stores the snapshot in memory.
func (m *MemoryStorage) SaveSnapshot(snapshot *types.Snapshot) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	copied := *snapshot
	data := make([]byte, len(snapshot.Data))
	copy(data, snapshot.Data)
	copied.Data = data

	cfg := make(map[types.ServerID]bool, len(snapshot.ServerConfig))
	for k, v := range snapshot.ServerConfig {
		cfg[k] = v
	}
	copied.ServerConfig = cfg

	m.snapshot = &copied
	return nil
}

// LoadSnapshot retrieves the in-memory snapshot. Returns nil if none exists.
func (m *MemoryStorage) LoadSnapshot() (*types.Snapshot, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.snapshot == nil {
		return nil, nil
	}

	copied := *m.snapshot
	data := make([]byte, len(m.snapshot.Data))
	copy(data, m.snapshot.Data)
	copied.Data = data

	cfg := make(map[types.ServerID]bool, len(m.snapshot.ServerConfig))
	for k, v := range m.snapshot.ServerConfig {
		cfg[k] = v
	}
	copied.ServerConfig = cfg

	return &copied, nil
}

// Ensure MemoryStorage implements the Storage interface at compile time.
var _ Storage = (*MemoryStorage)(nil)
