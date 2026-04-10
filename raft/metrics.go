package raft

import (
	"sync"
	"sync/atomic"
	"time"
)

// Metrics collects runtime performance counters for a RaftNode.
// All methods are safe for concurrent use.
type Metrics struct {
	// Apply latency tracking.
	applyCount   atomic.Int64
	applyTotalNs atomic.Int64 // cumulative nanoseconds
	applyMinNs   atomic.Int64
	applyMaxNs   atomic.Int64

	// Election tracking.
	electionCount   atomic.Int64
	lastElectionNs  atomic.Int64 // duration of the last successful election
	leaderChanges   atomic.Int64
	leaderSince     atomic.Int64 // UnixNano when this node became leader (0 if not leader)

	// Replication tracking.
	committedEntries atomic.Int64

	// Histogram: buckets for apply latency distribution.
	histMu   sync.Mutex
	histData []time.Duration // raw samples (capped)
}

// newMetrics creates a zeroed Metrics.
func newMetrics() *Metrics {
	m := &Metrics{
		histData: make([]time.Duration, 0, 8192),
	}
	m.applyMinNs.Store(int64(time.Hour)) // sentinel high value
	return m
}

// recordApply records one Apply round-trip duration.
func (m *Metrics) recordApply(d time.Duration) {
	ns := int64(d)
	m.applyCount.Add(1)
	m.applyTotalNs.Add(ns)

	// Update min (CAS loop).
	for {
		cur := m.applyMinNs.Load()
		if ns >= cur || m.applyMinNs.CompareAndSwap(cur, ns) {
			break
		}
	}
	// Update max (CAS loop).
	for {
		cur := m.applyMaxNs.Load()
		if ns <= cur || m.applyMaxNs.CompareAndSwap(cur, ns) {
			break
		}
	}

	m.histMu.Lock()
	if len(m.histData) < cap(m.histData) {
		m.histData = append(m.histData, d)
	}
	m.histMu.Unlock()
}

func (m *Metrics) recordElection(d time.Duration) {
	m.electionCount.Add(1)
	m.lastElectionNs.Store(int64(d))
	m.leaderChanges.Add(1)
}

func (m *Metrics) recordLeaderStart() {
	m.leaderSince.Store(time.Now().UnixNano())
}

func (m *Metrics) recordLeaderStop() {
	m.leaderSince.Store(0)
}

func (m *Metrics) recordCommit() {
	m.committedEntries.Add(1)
}

// ---- Public getters ----

// ApplyCount returns the total number of Apply calls completed.
func (m *Metrics) ApplyCount() int64 { return m.applyCount.Load() }

// ApplyAvg returns the average Apply latency.
func (m *Metrics) ApplyAvg() time.Duration {
	c := m.applyCount.Load()
	if c == 0 {
		return 0
	}
	return time.Duration(m.applyTotalNs.Load() / c)
}

// ApplyMin returns the minimum Apply latency observed.
func (m *Metrics) ApplyMin() time.Duration {
	v := m.applyMinNs.Load()
	if v == int64(time.Hour) {
		return 0
	}
	return time.Duration(v)
}

// ApplyMax returns the maximum Apply latency observed.
func (m *Metrics) ApplyMax() time.Duration { return time.Duration(m.applyMaxNs.Load()) }

// ElectionCount returns the number of elections won by this node.
func (m *Metrics) ElectionCount() int64 { return m.electionCount.Load() }

// LastElectionDuration returns how long the last election took.
func (m *Metrics) LastElectionDuration() time.Duration {
	return time.Duration(m.lastElectionNs.Load())
}

// LeaderChanges returns the total number of leader transitions observed.
func (m *Metrics) LeaderChanges() int64 { return m.leaderChanges.Load() }

// CommittedEntries returns the total number of entries committed.
func (m *Metrics) CommittedEntries() int64 { return m.committedEntries.Load() }

// Percentile returns the p-th percentile of Apply latency (0-100).
func (m *Metrics) Percentile(p float64) time.Duration {
	m.histMu.Lock()
	defer m.histMu.Unlock()
	n := len(m.histData)
	if n == 0 {
		return 0
	}
	// Copy and sort.
	sorted := make([]time.Duration, n)
	copy(sorted, m.histData)
	sortDurations(sorted)
	idx := int(p / 100.0 * float64(n))
	if idx >= n {
		idx = n - 1
	}
	return sorted[idx]
}

// sortDurations sorts a slice of durations (insertion sort, fine for <=8192 elements).
func sortDurations(d []time.Duration) {
	for i := 1; i < len(d); i++ {
		key := d[i]
		j := i - 1
		for j >= 0 && d[j] > key {
			d[j+1] = d[j]
			j--
		}
		d[j+1] = key
	}
}
