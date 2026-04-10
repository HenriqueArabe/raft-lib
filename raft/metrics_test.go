package raft

import (
	"testing"
	"time"
)

func TestMetricsApplyTracking(t *testing.T) {
	m := newMetrics()

	m.recordApply(10 * time.Millisecond)
	m.recordApply(20 * time.Millisecond)
	m.recordApply(30 * time.Millisecond)

	if m.ApplyCount() != 3 {
		t.Fatalf("expected 3 applies, got %d", m.ApplyCount())
	}

	avg := m.ApplyAvg()
	if avg < 19*time.Millisecond || avg > 21*time.Millisecond {
		t.Fatalf("expected avg ~20ms, got %s", avg)
	}

	if m.ApplyMin() != 10*time.Millisecond {
		t.Fatalf("expected min 10ms, got %s", m.ApplyMin())
	}

	if m.ApplyMax() != 30*time.Millisecond {
		t.Fatalf("expected max 30ms, got %s", m.ApplyMax())
	}

	p50 := m.Percentile(50)
	if p50 != 20*time.Millisecond {
		t.Fatalf("expected p50 20ms, got %s", p50)
	}
}

func TestMetricsElectionTracking(t *testing.T) {
	m := newMetrics()

	m.recordElection(500 * time.Millisecond)
	m.recordLeaderStart()

	if m.ElectionCount() != 1 {
		t.Fatalf("expected 1 election, got %d", m.ElectionCount())
	}
	if m.LastElectionDuration() != 500*time.Millisecond {
		t.Fatalf("expected 500ms, got %s", m.LastElectionDuration())
	}
	if m.LeaderChanges() != 1 {
		t.Fatalf("expected 1 leader change, got %d", m.LeaderChanges())
	}
}

func TestMetricsCommitTracking(t *testing.T) {
	m := newMetrics()

	for i := 0; i < 100; i++ {
		m.recordCommit()
	}

	if m.CommittedEntries() != 100 {
		t.Fatalf("expected 100 commits, got %d", m.CommittedEntries())
	}
}
