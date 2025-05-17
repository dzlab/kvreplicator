package kvreplicator

import (
	"fmt"
	"log"
	"os"
	"time"
)

const (
	defaultApplyTimeout        = 10 * time.Second
	defaultRetainSnapshotCount = 2
	defaultRaftLogCacheSize    = 512
	defaultSnapshotInterval    = 20 * time.Second
	defaultSnapshotThreshold   = 50
	defaultTCPMaxPool          = 3
	defaultTCPTimeout          = 10 * time.Second
	defaultHeartbeatTimeout    = 1 * time.Second
	defaultElectionTimeout     = 1 * time.Second
	defaultLeaderLeaseTimeout  = 500 * time.Millisecond
	defaultCommitTimeout       = 50 * time.Millisecond
)

// Config holds the configuration for the KVReplicator.
type Config struct {
	// Required fields
	NodeID          string // Unique ID for this node in the Raft cluster
	RaftBindAddress string // TCP address for Raft to bind to (e.g., "localhost:7000")
	RaftDataDir     string // Directory to store Raft log and snapshots
	DBPath          string // Path for PebbleDB data

	// Optional fields with defaults
	Bootstrap     bool          // Whether to bootstrap a new cluster if no existing state
	JoinAddresses []string      // Addresses of existing cluster members to join
	ApplyTimeout  time.Duration // Timeout for Raft apply operations
	Logger        *log.Logger   // Logger instance

	// Advanced Raft configuration
	SnapshotInterval    time.Duration // How often to check if we should perform a snapshot
	SnapshotThreshold   uint64        // How many Raft logs trigger a snapshot
	RetainSnapshotCount int           // How many snapshots to keep
	RaftLogCacheSize    int           // Size of the Raft log cache
	TCPMaxPool          int           // Maximum number of connections in TCP pool
	TCPTimeout          time.Duration // TCP timeout for establishing connections
	HeartbeatTimeout    time.Duration // Heartbeat timeout for Raft
	ElectionTimeout     time.Duration // Election timeout for Raft
	LeaderLeaseTimeout  time.Duration // Leader lease timeout
	CommitTimeout       time.Duration // How long to wait for commits
}

// DefaultConfig returns a Config with sensible defaults for most settings.
func DefaultConfig(nodeID, raftBindAddr, raftDataDir, dbPath string) Config {
	return Config{
		NodeID:              nodeID,
		RaftBindAddress:     raftBindAddr,
		RaftDataDir:         raftDataDir,
		DBPath:              dbPath,
		Bootstrap:           false,
		JoinAddresses:       []string{},
		ApplyTimeout:        defaultApplyTimeout,
		Logger:              log.New(os.Stdout, fmt.Sprintf("[%s] ", nodeID), log.LstdFlags|log.Lmicroseconds),
		SnapshotInterval:    defaultSnapshotInterval,
		SnapshotThreshold:   defaultSnapshotThreshold,
		RetainSnapshotCount: defaultRetainSnapshotCount,
		RaftLogCacheSize:    defaultRaftLogCacheSize,
		TCPMaxPool:          defaultTCPMaxPool,
		TCPTimeout:          defaultTCPTimeout,
		HeartbeatTimeout:    defaultHeartbeatTimeout,
		ElectionTimeout:     defaultElectionTimeout,
		LeaderLeaseTimeout:  defaultLeaderLeaseTimeout,
		CommitTimeout:       defaultCommitTimeout,
	}
}

// ValidateConfig checks if the configuration is valid and complete
func ValidateConfig(cfg *Config) error {
	if cfg.NodeID == "" {
		return fmt.Errorf("configuration error: NodeID must be specified")
	}
	if cfg.RaftBindAddress == "" {
		return fmt.Errorf("configuration error: RaftBindAddress must be specified")
	}
	if cfg.RaftDataDir == "" {
		return fmt.Errorf("configuration error: RaftDataDir must be specified")
	}
	if cfg.DBPath == "" {
		return fmt.Errorf("configuration error: DBPath must be specified")
	}
	return nil
}

// WithClusterConfig configures RAFT Cluster parameters
func (c Config) WithClusterConfig(bootstrapCluster bool, joinAddresses []string) Config {
	c.Bootstrap = bootstrapCluster
	c.JoinAddresses = joinAddresses
	return c
}

// WithRaftConfig configures advanced Raft parameters
func (c Config) WithRaftConfig(heartbeat, election, leaderLease, commit time.Duration) Config {
	c.HeartbeatTimeout = heartbeat
	c.ElectionTimeout = election
	c.LeaderLeaseTimeout = leaderLease
	c.CommitTimeout = commit
	return c
}

// WithRaftSnapshotConfig configures Raft snapshot parameters
func (c Config) WithRaftSnapshotConfig(interval time.Duration, threshold uint64, retainCount int) Config {
	c.SnapshotInterval = interval
	c.SnapshotThreshold = threshold
	c.RetainSnapshotCount = retainCount
	return c
}

// WithTransportConfig configures TCP transport parameters
func (c Config) WithTransportConfig(maxPool int, timeout time.Duration) Config {
	c.TCPMaxPool = maxPool
	c.TCPTimeout = timeout
	return c
}
