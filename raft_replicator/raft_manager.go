package raft_replicator

import (
	"fmt"
	"log"
	"net"
	"os"
	"path/filepath"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb"
)

// raftManager handles the lifecycle and operations of the Raft node.
type raftManager struct {
	config    Config
	raftNode  *raft.Raft
	transport raft.Transport
	logger    *log.Logger
}

// newRaftManager initializes and returns a new raftManager.
// It sets up the Raft configuration, transport, stores, and creates the Raft instance.
// The Raft node is not started automatically; call Start() for that.
func newRaftManager(cfg Config, fsm raft.FSM, logger *log.Logger) (*raftManager, error) {
	// Ensure Raft data directory exists
	if err := os.MkdirAll(cfg.RaftDataDir, 0700); err != nil {
		return nil, fmt.Errorf("failed to create Raft data directory %s: %w", cfg.RaftDataDir, err)
	}

	// Setup Raft configuration.
	raftConfig := raft.DefaultConfig()
	raftConfig.LocalID = raft.ServerID(cfg.NodeID)
	raftConfig.Logger = hclog.New(&hclog.LoggerOptions{
		Name:   fmt.Sprintf("raft-%s", cfg.NodeID),
		Output: logger.Writer(),
		Level:  hclog.Info, // Or hclog.Debug
	})
	raftConfig.SnapshotInterval = cfg.SnapshotInterval
	raftConfig.SnapshotThreshold = cfg.SnapshotThreshold
	if cfg.HeartbeatTimeout > 0 {
		raftConfig.HeartbeatTimeout = cfg.HeartbeatTimeout
	}
	if cfg.ElectionTimeout > 0 {
		raftConfig.ElectionTimeout = cfg.ElectionTimeout
	}
	if cfg.LeaderLeaseTimeout > 0 {
		raftConfig.LeaderLeaseTimeout = cfg.LeaderLeaseTimeout
	}
	if cfg.CommitTimeout > 0 {
		raftConfig.CommitTimeout = cfg.CommitTimeout
	}

	// Setup Raft communication (transport).
	addr, err := net.ResolveTCPAddr("tcp", cfg.RaftBindAddress)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve Raft bind address %s: %w", cfg.RaftBindAddress, err)
	}
	transport, err := raft.NewTCPTransport(cfg.RaftBindAddress, addr, cfg.TCPMaxPool, cfg.TCPTimeout, logger.Writer())
	if err != nil {
		return nil, fmt.Errorf("failed to create Raft TCP transport: %w", err)
	}

	// Create snapshot store. This allows Raft to compact its log.
	snapshotStore, err := raft.NewFileSnapshotStore(cfg.RaftDataDir, cfg.RetainSnapshotCount, logger.Writer())
	if err != nil {
		return nil, fmt.Errorf("failed to create Raft file snapshot store in %s: %w", cfg.RaftDataDir, err)
	}

	// Create log store and stable store. BoltDB is a good default.
	boltDBPath := filepath.Join(cfg.RaftDataDir, "raft.db")
	logStore, err := raftboltdb.NewBoltStore(boltDBPath)
	if err != nil {
		return nil, fmt.Errorf("failed to create BoltDB store at %s: %w", boltDBPath, err)
	}
	stableStore := logStore // BoltStore implements both StableStore and LogStore

	// Instantiate the Raft system.
	r, err := raft.NewRaft(raftConfig, fsm, logStore, stableStore, snapshotStore, transport)
	if err != nil {
		return nil, fmt.Errorf("failed to create Raft instance: %w", err)
	}

	manager := &raftManager{
		config:    cfg,
		raftNode:  r,
		transport: transport,
		logger:    logger,
	}

	return manager, nil
}

// Start initializes the Raft node. If config.Bootstrap is true and no existing state,
// it bootstraps a new cluster.
func (rm *raftManager) Start() error {
	if rm.config.Bootstrap {
		configuration := raft.Configuration{
			Servers: []raft.Server{
				{
					ID:      raft.ServerID(rm.config.NodeID),
					Address: rm.transport.LocalAddr(),
				},
			},
		}
		bootstrapFuture := rm.raftNode.BootstrapCluster(configuration)
		if err := bootstrapFuture.Error(); err != nil {
			rm.logger.Printf("INFO: BootstrapCluster returned: %v (this may be normal if already bootstrapped or joining)", err)
		} else {
			rm.logger.Printf("INFO: Cluster bootstrapped successfully with node %s", rm.config.NodeID)
		}
	}

	// Handle joining logic (simplified as before, focused on logging intention)
	if len(rm.config.JoinAddresses) > 0 && !rm.config.Bootstrap {
		rm.logger.Printf("Attempting to join cluster via addresses: %v", rm.config.JoinAddresses)
		// Actual joining mechanism typically involves leader adding this node.
		// Raft library handles discovery if node is configured as a peer.
		// For now, log the intent; AddVoter API can be used on leader.
		rm.logger.Printf("INFO: If this node is not part of the initial cluster configuration, it must be added via AddVoter by the leader.")
	} else if !rm.config.Bootstrap {
		rm.logger.Printf("INFO: Starting node. Will attempt to discover existing cluster or wait to be added.")
	}

	rm.logger.Printf("Raft node started. Node ID: %s, Address: %s", rm.config.NodeID, rm.config.RaftBindAddress)
	return nil
}

// Shutdown gracefully shuts down the Raft node.
func (rm *raftManager) Shutdown() error {
	rm.logger.Println("Shutting down Raft node...")
	shutdownFuture := rm.raftNode.Shutdown()
	if err := shutdownFuture.Error(); err != nil {
		rm.logger.Printf("ERROR: failed to shut down Raft node: %v", err)
		return err // Return error
	}
	rm.logger.Println("Raft node shut down.")
	return nil
}

// ApplyCommand applies a command to the FSM via Raft.
func (rm *raftManager) ApplyCommand(cmd *Command, timeout time.Duration) error {
	data, err1 := cmd.Serialize()
	if err1 != nil {
		return fmt.Errorf("failed to serialize %s command: %w", cmd.Op, err1)
	}
	applyFuture := rm.Apply(data, timeout)
	if err2 := applyFuture.Error(); err2 != nil {
		return fmt.Errorf("failed to apply %s command via Raft: %w", cmd.Op, err2)
	}
	// Check the response from the FSM application
	response := applyFuture.Response()
	if err3, ok := response.(error); ok {
		return fmt.Errorf("failed to apply %s command via Raft: %w", cmd.Op, err3)
	}
	// Log success after confirming FSM application
	rm.logger.Printf("%s successful: Key=%s", cmd.Op, cmd.Key)
	return nil
}

// This is the core mechanism for replicating state changes.
func (rm *raftManager) Apply(data []byte, timeout time.Duration) raft.ApplyFuture {
	return rm.raftNode.Apply(data, timeout)
}

// IsLeader checks if the current node is the Raft leader.
func (rm *raftManager) IsLeader() bool {
	return rm.raftNode.State() == raft.Leader
}

// Leader returns the Raft address of the current leader.
// Returns empty string if there is no current leader.
func (rm *raftManager) Leader() raft.ServerAddress {
	return rm.raftNode.Leader()
}

// Stats returns basic stats from the Raft node.
func (rm *raftManager) Stats() map[string]string {
	if rm.raftNode == nil {
		return map[string]string{"error": "Raft node not initialized"}
	}
	return rm.raftNode.Stats()
}

// AddVoter attempts to add a new node to the cluster as a voter.
// This must be run on the leader.
// serverID: ID of the new node.
// serverAddress: Address of the new node.
func (rm *raftManager) AddVoter(serverID string, serverAddress string) error {
	// The leader check is handled by the caller (KVReplicator).
	// This method just performs the raft operation.

	rm.logger.Printf("Attempting to add voter: ID=%s, Address=%s", serverID, serverAddress)

	// Raft's AddVoter handles checking if the server already exists internally
	// and is idempotent if the server ID and address match.
	// We can simplify by removing the manual check here, letting Raft handle it.

	// The index 0 ensures the command is applied immediately.
	addFuture := rm.raftNode.AddVoter(raft.ServerID(serverID), raft.ServerAddress(serverAddress), 0, 0)
	if err := addFuture.Error(); err != nil {
		return fmt.Errorf("failed to add voter %s at %s: %w", serverID, serverAddress, err)
	}

	rm.logger.Printf("Successfully added voter: ID=%s, Address=%s", serverID, serverAddress)
	return nil
}

// RemoveServer attempts to remove a node from the cluster.
// This must be run on the leader.
// serverID: ID of the node to remove.
func (rm *raftManager) RemoveServer(serverID string) error {
	// The leader check and self-removal check are handled by the caller (KVReplicator).
	// This method just performs the raft operation.

	rm.logger.Printf("Attempting to remove server: ID=%s", serverID)

	removeFuture := rm.raftNode.RemoveServer(raft.ServerID(serverID), 0, 0)
	if err := removeFuture.Error(); err != nil {
		return fmt.Errorf("failed to remove server %s: %w", serverID, err)
	}

	rm.logger.Printf("Successfully removed server: ID=%s", serverID)
	return nil
}
