package raft

import (
	"fmt"
	"log/slog"

	"github.com/henrique-arab/raft-lib/types"
)

// ---- nodeState configuration methods -------------------------------

// addConfigLog appends a LogConfiguration entry to the leader's log.
// Also immediately updates serverConfig (Ongaro's simplified single-server method).
func (s *nodeState) addConfigLog(leaderID types.ServerID, cfg types.ConfigChange) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	lastIdx, _ := s.getLastLogIdxTerm()
	entry := types.RaftLog{
		Index:  lastIdx + 1,
		Term:   s.currentTerm,
		Type:   types.LogConfiguration,
		Config: cfg,
	}

	s.applyConfigurationLocked(lastIdx+1, cfg)

	if s.nextLogIdx >= logArrayCapacity {
		if !s.takeSnapshot() || s.nextLogIdx >= logArrayCapacity {
			return false
		}
	}

	s.logs[s.nextLogIdx] = entry
	s.logHashes[s.nextLogIdx] = s.getLogHash(entry)
	s.nextLogIdx++

	slog.Info(fmt.Sprintf("State - add config log: idx=%d add=%v server=%s",
		entry.Index, cfg.Add, cfg.Server))

	s.matchIndex[leaderID] = entry.Index
	s.lastSentLogIndex[leaderID] = entry.Index
	s.nextIndex[leaderID] = entry.Index + 1
	return true
}

// applyConfigurationLocked updates serverConfig for a new configuration entry.
// mu must be held by the caller.
func (s *nodeState) applyConfigurationLocked(logIdx int, cfg types.ConfigChange) {
	// Preserve old config for potential rollback.
	s.oldServerConfig = copyConfig(s.serverConfig)
	if cfg.Add {
		s.serverConfig[cfg.Server] = true
	} else {
		delete(s.serverConfig, cfg.Server)
	}
	s.lastConfigLogIndex = logIdx
}

// handleConfigurationRPC enqueues a membership change request and tries to
// start processing it immediately if no change is in progress.
// Returns (ok, entry) where ok == true means this call started the change.
func (s *nodeState) handleConfigurationRPC(entry configEntry) (bool, configEntry) {
	s.mu.Lock()
	s.configQueue = append(s.configQueue, entry)
	s.pendingConfigs++
	slog.Debug(fmt.Sprintf("Config queue size: %d", s.pendingConfigs))
	s.mu.Unlock()

	return s.handleNextConfigurationChange()
}

// handleNextConfigurationChange pops the next pending config change and starts
// processing it (adds a config log entry) if not already in a change.
// Returns (ok, entry) where ok == true means a new change was started.
func (s *nodeState) handleNextConfigurationChange() (bool, configEntry) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.inConfigChange || s.pendingConfigs == 0 {
		return false, configEntry{}
	}

	s.inConfigChange = true
	entry := s.configQueue[0]
	copy(s.configQueue, s.configQueue[1:])
	s.configQueue = s.configQueue[:len(s.configQueue)-1]
	s.pendingConfigs--

	// We need the leader ID to update the leader's own matchIndex/nextIndex.
	// Pass "" here; addConfigLog will compute it using currentLeader.
	s.addConfigLogUnlocked(s.currentLeader, entry.msg)

	slog.Debug(fmt.Sprintf("Config queue remaining: %d", s.pendingConfigs))
	return true, entry
}

// addConfigLogUnlocked is addConfigLog without locking (mu already held).
func (s *nodeState) addConfigLogUnlocked(leaderID types.ServerID, cfg types.ConfigChange) {
	lastIdx, _ := s.getLastLogIdxTerm()
	entry := types.RaftLog{
		Index:  lastIdx + 1,
		Term:   s.currentTerm,
		Type:   types.LogConfiguration,
		Config: cfg,
	}
	s.applyConfigurationLocked(lastIdx+1, cfg)
	if s.nextLogIdx >= logArrayCapacity {
		return
	}
	s.logs[s.nextLogIdx] = entry
	s.logHashes[s.nextLogIdx] = s.getLogHash(entry)
	s.nextLogIdx++
	slog.Info(fmt.Sprintf("State - add config log: idx=%d add=%v server=%s",
		entry.Index, cfg.Add, cfg.Server))
	s.matchIndex[leaderID] = entry.Index
	s.lastSentLogIndex[leaderID] = entry.Index
	s.nextIndex[leaderID] = entry.Index + 1
}

// unlockNextConfiguration marks the current config change as complete and
// checks if there is a queued change to start.
func (s *nodeState) unlockNextConfiguration() {
	s.mu.Lock()
	s.inConfigChange = false
	s.mu.Unlock()
}

// addNewServer initialises leader tracking state for a newly connected peer.
func (s *nodeState) addNewServer(id types.ServerID) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, found := s.nextIndex[id]; !found {
		s.lastSentLogIndex[id] = 0
		s.nextIndex[id] = 0
		s.matchIndex[id] = 0
	}
}

// removeServer removes a server from configuration and clears its tracking.
func (s *nodeState) removeServer(id types.ServerID) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.serverConfig, id)
	delete(s.clientLastSeq, id)
}

// serverInConfiguration returns true if id is a voting member.
func (s *nodeState) serverInConfiguration(id types.ServerID) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, found := s.serverConfig[id]
	return found
}

// addUnvotingServer registers a joining server that is catching up.
func (s *nodeState) addUnvotingServer(entry configEntry) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.unvotingServers[entry.msg.Server] = entry
}

// removeUnvotingServer removes and returns the configEntry for the given server.
func (s *nodeState) removeUnvotingServer(id types.ServerID) (configEntry, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	entry, found := s.unvotingServers[id]
	if found {
		delete(s.unvotingServers, id)
	}
	return entry, found
}

// getUnvotingServerIDs returns a snapshot of the current unvoting server IDs.
func (s *nodeState) getUnvotingServerIDs() []types.ServerID {
	s.mu.Lock()
	defer s.mu.Unlock()
	ids := make([]types.ServerID, 0, len(s.unvotingServers))
	for id := range s.unvotingServers {
		ids = append(ids, id)
	}
	return ids
}

// ---- RaftNode configuration helpers --------------------------------

// handleConfigurationMessages is the goroutine that processes membership RPCs.
func (n *RaftNode) handleConfigurationMessages() {
	for {
		select {
		case req := <-n.confChan:
			switch n.state.getRole() {
			case Follower, Candidate:
				req.resultCh <- &types.AddRemoveServerResponse{
					Success:  false,
					LeaderID: n.state.getCurrentLeader(),
				}
			case Leader:
				slog.Debug(fmt.Sprintf("Leader received config change: add=%v server=%s",
					req.msg.Add, req.msg.Server))
				if req.msg.Add {
					// Connect to the new server so we can send it log entries.
					if err := n.transport.Connect(req.msg.Server); err != nil {
						slog.Warn(fmt.Sprintf("Connect to new server %s: %v", req.msg.Server, err))
					}
					n.state.addNewServer(req.msg.Server)
					n.state.addUnvotingServer(req)
					// Start replicating immediately so the new server catches up.
					n.sendAppendEntriesRPCs()
				} else {
					ok, entry := n.state.handleConfigurationRPC(req)
					if ok {
						logIdx := n.state.getLastConfigLogIndex()
						n.pendingConfigMu.Lock()
						n.pendingConfigApplied[logIdx] = entry.chanApplied
						n.pendingConfigMu.Unlock()
						go n.waitForConfigApplied(entry)
					}
				}
			}
		case <-n.stopCh:
			return
		}
	}
}

// waitForConfigApplied waits for a configuration log to be applied, then
// signals the caller via the result channel.
func (n *RaftNode) waitForConfigApplied(entry configEntry) {
	const timeout = 500
	select {
	case <-entry.chanApplied:
		entry.resultCh <- &types.AddRemoveServerResponse{
			Success:  true,
			LeaderID: n.state.getCurrentLeader(),
		}
	case <-n.stopCh:
	}
}

// promoteUnvotingServer moves a caught-up joining server into the voting
// configuration by adding it to the configQueue.
func (n *RaftNode) promoteUnvotingServer(id types.ServerID) {
	entry, found := n.state.removeUnvotingServer(id)
	if !found {
		return
	}
	ok, conf := n.state.handleConfigurationRPC(entry)
	if ok {
		logIdx := n.state.getLastConfigLogIndex()
		n.pendingConfigMu.Lock()
		n.pendingConfigApplied[logIdx] = conf.chanApplied
		n.pendingConfigMu.Unlock()
		go n.waitForConfigApplied(conf)
	}
}

// ---- Utility -------------------------------------------------------

// copyConfig returns a shallow copy of a server configuration map.
func copyConfig(src map[types.ServerID]bool) map[types.ServerID]bool {
	dst := make(map[types.ServerID]bool, len(src))
	for k, v := range src {
		dst[k] = v
	}
	return dst
}
