package kvreplicator

import (
	"fmt"
	"log"
	"net"
	"os"
	"path/filepath"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb" // Using BoltDB for log and stable store
)

const (
	defaultApplyTimeout        = 10 * time.Second
	defaultRetainSnapshotCount = 2
	defaultRaftLogCacheSize    = 512
)

// Config holds the configuration for the KVReplicator.
type Config struct {
	NodeID          string        // Unique ID for this node in the Raft cluster.
	RaftBindAddress string        // TCP address for Raft to bind to (e.g., "localhost:7000").
	RaftDataDir     string        // Directory to store Raft log and snapshots.
	DBPath          string        // Path for PebbleDB data.
	Bootstrap       bool          // Whether to bootstrap a new cluster if no existing state.
	JoinAddresses   []string      // Addresses of existing cluster members to join (optional).
	ApplyTimeout    time.Duration // Timeout for Raft apply operations.
	Logger          *log.Logger   // Logger instance.
}

// KVReplicator provides a replicated key-value store using PebbleDB and Raft.
type KVReplicator struct {
	config    Config
	raftNode  *raft.Raft
	fsm       *pebbleFSM // Changed from rocksDBFSM
	logger    *log.Logger
	transport raft.Transport
}

// NewKVReplicator creates and initializes a new KVReplicator instance.
// It sets up the FSM, Raft node, transport, log stores, and snapshot store.
// The Raft node is not started automatically; call Start() for that.
func NewKVReplicator(cfg Config) (*KVReplicator, error) {
	if cfg.Logger == nil {
		cfg.Logger = log.New(os.Stderr, "[kvreplicator] ", log.LstdFlags|log.Lmicroseconds)
	}
	if cfg.ApplyTimeout == 0 {
		cfg.ApplyTimeout = defaultApplyTimeout
	}
	if cfg.RaftDataDir == "" {
		return nil, fmt.Errorf("RaftDataDir must be specified")
	}
	if cfg.NodeID == "" {
		return nil, fmt.Errorf("NodeID must be specified")
	}
	if cfg.DBPath == "" {
		return nil, fmt.Errorf("DBPath must be specified")
	}

	// Ensure Raft data directory exists
	if err := os.MkdirAll(cfg.RaftDataDir, 0700); err != nil {
		return nil, fmt.Errorf("failed to create Raft data directory %s: %w", cfg.RaftDataDir, err)
	}
	// Pebble FSM will create its own directory based on DBPath

	kv := &KVReplicator{
		config: cfg,
		logger: cfg.Logger,
	}

	// Initialize the FSM (Finite State Machine) for Pebble
	fsm, err := newPebbleFSM(cfg.DBPath, cfg.Logger)
	if err != nil {
		return nil, fmt.Errorf("failed to create Pebble FSM: %w", err)
	}
	kv.fsm = fsm

	// Setup Raft configuration.
	raftConfig := raft.DefaultConfig()
	raftConfig.LocalID = raft.ServerID(cfg.NodeID)
	raftConfig.Logger = hclog.New(&hclog.LoggerOptions{
		Name:   fmt.Sprintf("raft-%s", cfg.NodeID),
		Output: cfg.Logger.Writer(), // Standard logger's output
		Level:  hclog.Info,          // Or hclog.Debug for more verbosity
	})
	raftConfig.SnapshotInterval = 20 * time.Second
	raftConfig.SnapshotThreshold = 50 // Number of logs before a snapshot

	// Setup Raft communication (transport).
	addr, err := net.ResolveTCPAddr("tcp", cfg.RaftBindAddress)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve Raft bind address %s: %w", cfg.RaftBindAddress, err)
	}
	transport, err := raft.NewTCPTransport(cfg.RaftBindAddress, addr, 3, 10*time.Second, cfg.Logger.Writer())
	if err != nil {
		return nil, fmt.Errorf("failed to create Raft TCP transport: %w", err)
	}
	kv.transport = transport // Store the transport

	// Create snapshot store. This allows Raft to compact its log.
	snapshotStore, err := raft.NewFileSnapshotStore(cfg.RaftDataDir, defaultRetainSnapshotCount, cfg.Logger.Writer())
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
	r, err := raft.NewRaft(raftConfig, kv.fsm, logStore, stableStore, snapshotStore, transport)
	if err != nil {
		// Close FSM if raft creation fails
		if closeErr := kv.fsm.Close(); closeErr != nil {
			kv.logger.Printf("ERROR: failed to close FSM after Raft creation error: %v", closeErr)
		}
		return nil, fmt.Errorf("failed to create Raft instance: %w", err)
	}
	kv.raftNode = r

	return kv, nil
}

// Start initializes the Raft node. If config.Bootstrap is true and no existing state,
// it bootstraps a new cluster.
func (kv *KVReplicator) Start() error {
	if kv.config.Bootstrap {
		configuration := raft.Configuration{
			Servers: []raft.Server{
				{
					ID:      raft.ServerID(kv.config.NodeID),
					Address: kv.transport.LocalAddr(),
				},
			},
		}
		bootstrapFuture := kv.raftNode.BootstrapCluster(configuration)
		if err := bootstrapFuture.Error(); err != nil {
			kv.logger.Printf("INFO: BootstrapCluster returned: %v (this may be normal if already bootstrapped or joining)", err)
		} else {
			kv.logger.Printf("INFO: Cluster bootstrapped successfully with node %s", kv.config.NodeID)
		}
	}

	// Handle joining logic (simplified as before, focused on logging intention)
	if len(kv.config.JoinAddresses) > 0 && !kv.config.Bootstrap {
		kv.logger.Printf("Attempting to join cluster via addresses: %v", kv.config.JoinAddresses)
		// Actual joining mechanism typically involves leader adding this node.
		// Raft library handles discovery if node is configured as a peer.
		// For now, log the intent; AddVoter API can be used on leader.
		kv.logger.Printf("INFO: If this node is not part of the initial cluster configuration, it must be added via AddVoter by the leader.")
	} else if !kv.config.Bootstrap {
		kv.logger.Printf("INFO: Starting node. Will attempt to discover existing cluster or wait to be added.")
	}

	kv.logger.Printf("KVReplicator started. Node ID: %s, Raft Address: %s, Pebble Path: %s", kv.config.NodeID, kv.config.RaftBindAddress, kv.config.DBPath)
	return nil
}

// Get retrieves a value for a given key.
// Reads directly from the local FSM (PebbleDB).
func (kv *KVReplicator) Get(key string) (string, error) {
	// pebbleFSM.Get now returns (string, error), where error can be "key not found"
	value, err := kv.fsm.Get(key)
	if err != nil {
		// The FSM's Get method already formats the "key not found" error,
		// so we can return it directly.
		return "", err
	}
	return value, nil
}

// Put sets a value for a given key. The operation is applied via Raft log.
func (kv *KVReplicator) Put(key, value string) error {
	if kv.raftNode.State() != raft.Leader {
		leaderAddr := kv.raftNode.Leader()
		return fmt.Errorf("cannot apply Put: not the leader. Current leader: %s", leaderAddr)
	}

	cmd := &Command{
		Op:    OpPut,
		Key:   key,
		Value: value,
	}
	data, err := cmd.Serialize()
	if err != nil {
		return fmt.Errorf("failed to serialize Put command: %w", err)
	}

	applyFuture := kv.raftNode.Apply(data, kv.config.ApplyTimeout)
	if err := applyFuture.Error(); err != nil {
		return fmt.Errorf("failed to apply Put command via Raft: %w", err)
	}

	response := applyFuture.Response()
	if err, ok := response.(error); ok {
		return fmt.Errorf("put command failed to apply on FSM: %w", err)
	}

	kv.logger.Printf("Put successful: Key=%s", key)
	return nil
}

// Delete removes a key from the store. The operation is applied via Raft log.
func (kv *KVReplicator) Delete(key string) error {
	if kv.raftNode.State() != raft.Leader {
		leaderAddr := kv.raftNode.Leader()
		return fmt.Errorf("cannot apply Delete: not the leader. Current leader: %s", leaderAddr)
	}

	cmd := &Command{
		Op:  OpDelete,
		Key: key,
	}
	data, err := cmd.Serialize()
	if err != nil {
		return fmt.Errorf("failed to serialize %s command: %w", cmd.Op, err)
	}

	applyFuture := kv.raftNode.Apply(data, kv.config.ApplyTimeout)
	if err := applyFuture.Error(); err != nil {
		return fmt.Errorf("failed to apply %s command via Raft: %w", cmd.Op, err)
	}

	response := applyFuture.Response()
	if err, ok := response.(error); ok {
		return fmt.Errorf("%s command failed to apply on FSM: %w", cmd.Op, err)
	}

	kv.logger.Printf("%s successful: Key=%s", cmd.Op, key)
	return nil
}

// IsLeader checks if the current node is the Raft leader.
func (kv *KVReplicator) IsLeader() bool {
	return kv.raftNode.State() == raft.Leader
}

// Leader returns the Raft address of the current leader.
// Returns empty string if there is no current leader.
func (kv *KVReplicator) Leader() raft.ServerAddress {
	return kv.raftNode.Leader()
}

// Shutdown gracefully shuts down the KVReplicator, including the Raft node and FSM.
func (kv *KVReplicator) Shutdown() error {
	kv.logger.Println("Shutting down KVReplicator...")

	shutdownFuture := kv.raftNode.Shutdown()
	if err := shutdownFuture.Error(); err != nil {
		kv.logger.Printf("ERROR: failed to shut down Raft node: %v", err)
		// Continue to close FSM even if Raft shutdown has issues
	} else {
		kv.logger.Println("Raft node shut down.")
	}

	if kv.fsm != nil {
		if err := kv.fsm.Close(); err != nil {
			kv.logger.Printf("ERROR: failed to close FSM (Pebble DB): %v", err)
			return err // Or collect errors and return aggregate
		}
		kv.logger.Println("FSM (Pebble DB) closed.")
	}
	return nil
}

// Stats returns basic stats from the Raft node.
func (kv *KVReplicator) Stats() map[string]string {
	if kv.raftNode == nil {
		return map[string]string{"error": "Raft node not initialized"}
	}
	return kv.raftNode.Stats()
}

// AddVoter attempts to add a new node to the cluster as a voter.
// This must be run on the leader.
// serverID: ID of the new node.
// serverAddress: Address of the new node.
func (kv *KVReplicator) AddVoter(serverID string, serverAddress string) error {
	if !kv.IsLeader() {
		leaderAddr := kv.Leader()
		return fmt.Errorf("node is not the leader, cannot add voter. Current leader: %s", leaderAddr)
	}

	kv.logger.Printf("Attempting to add voter: ID=%s, Address=%s", serverID, serverAddress)

	configFuture := kv.raftNode.GetConfiguration()
	if err := configFuture.Error(); err != nil {
		return fmt.Errorf("failed to get current raft configuration: %w", err)
	}
	currentConfig := configFuture.Configuration()

	for _, srv := range currentConfig.Servers {
		if srv.ID == raft.ServerID(serverID) {
			if srv.Address == raft.ServerAddress(serverAddress) {
				kv.logger.Printf("Node %s with address %s already part of configuration.", serverID, serverAddress)
				return nil // Already exists with same address
			}
			return fmt.Errorf("node %s already exists with a different address: %s", serverID, srv.Address)
		}
	}

	addFuture := kv.raftNode.AddVoter(raft.ServerID(serverID), raft.ServerAddress(serverAddress), 0, 0)
	if err := addFuture.Error(); err != nil {
		return fmt.Errorf("failed to add voter %s at %s: %w", serverID, serverAddress, err)
	}

	kv.logger.Printf("Successfully added voter: ID=%s, Address=%s", serverID, serverAddress)
	return nil
}

// RemoveServer attempts to remove a node from the cluster.
// This must be run on the leader.
// serverID: ID of the node to remove.
func (kv *KVReplicator) RemoveServer(serverID string) error {
	if !kv.IsLeader() {
		leaderAddr := kv.Leader()
		return fmt.Errorf("node is not the leader, cannot remove server. Current leader: %s", leaderAddr)
	}
	if serverID == kv.config.NodeID {
		return fmt.Errorf("cannot remove self from the cluster using this method; leader transfer might be needed")
	}

	kv.logger.Printf("Attempting to remove server: ID=%s", serverID)

	removeFuture := kv.raftNode.RemoveServer(raft.ServerID(serverID), 0, 0)
	if err := removeFuture.Error(); err != nil {
		return fmt.Errorf("failed to remove server %s: %w", serverID, err)
	}

	kv.logger.Printf("Successfully removed server: ID=%s", serverID)
	return nil
}
