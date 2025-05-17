package kvreplicator

import (
	"fmt"
	"log"
	"os"

	"github.com/hashicorp/raft"
	// Using BoltDB for log and stable store
)

// KVReplicator provides a replicated key-value store using PebbleDB and Raft.
type KVReplicator struct {
	config      Config
	raftManager *raftManager
	kvStore     *kvStore
	logger      *log.Logger
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
	if cfg.SnapshotInterval == 0 {
		cfg.SnapshotInterval = defaultSnapshotInterval
	}
	if cfg.SnapshotThreshold == 0 {
		cfg.SnapshotThreshold = defaultSnapshotThreshold
	}
	if cfg.RetainSnapshotCount == 0 {
		cfg.RetainSnapshotCount = defaultRetainSnapshotCount
	}
	if cfg.TCPMaxPool == 0 {
		cfg.TCPMaxPool = defaultTCPMaxPool
	}
	if cfg.TCPTimeout == 0 {
		cfg.TCPTimeout = defaultTCPTimeout
	}

	if err := ValidateConfig(&cfg); err != nil {
		return nil, err
	}

	kv := &KVReplicator{
		config: cfg,
		logger: cfg.Logger,
	}

	// Initialize the KV Store (which wraps the Pebble FSM)
	kvStore, err := newKVStore(cfg.DBPath, cfg.Logger)
	if err != nil {
		return nil, fmt.Errorf("failed to create KV Store: %w", err)
	}
	kv.kvStore = kvStore

	// Initialize the Raft Manager
	// The raftManager needs the FSM instance to apply logs.
	raftManager, err := newRaftManager(cfg, kv.kvStore.fsm, cfg.Logger) // Pass kvStore as raft.FSM
	if err != nil {
		// Close KV Store if raft manager creation fails
		if closeErr := kv.kvStore.Close(); closeErr != nil {
			kv.logger.Printf("ERROR: failed to close KV Store after Raft manager creation error: %v", closeErr)
		}
		return nil, fmt.Errorf("failed to create Raft manager: %w", err)
	}
	kv.raftManager = raftManager

	return kv, nil
}

// Start initializes the Raft node via the raftManager.
// If config.Bootstrap is true and no existing state, it bootstraps a new cluster.
func (kv *KVReplicator) Start() error {
	// The raftManager handles the bootstrap/join logic within its Start method.
	return kv.raftManager.Start()
	// The logger message about KVReplicator starting can be moved to main or called after this returns.
	// For now, leaving it out as raftManager Start logs its own start.
}

// Get retrieves a value for a given key.
// Reads directly from the local KV store (PebbleDB).
// This read does not go through Raft.
func (kv *KVReplicator) Get(key string) (string, error) {
	return kv.kvStore.Get(key)
}

// Put sets a value for a given key. The operation is applied via Raft log.
// // This method must be called on the leader node.
func (kv *KVReplicator) Put(key, value string) error {
	if !kv.raftManager.IsLeader() {
		leaderAddr := kv.raftManager.Leader()
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

	// Apply the command through the Raft manager
	applyFuture := kv.raftManager.ApplyCommand(data, kv.config.ApplyTimeout)
	if err := applyFuture.Error(); err != nil {
		return fmt.Errorf("failed to apply Put command via Raft: %w", err)
	}

	// Check the response from the FSM application
	response := applyFuture.Response()
	if err, ok := response.(error); ok {
		return fmt.Errorf("put command failed to apply on FSM: %w", err)
	}

	// Log success after confirming FSM application
	kv.logger.Printf("Put successful: Key=%s", key)
	return nil
}

// Delete removes a key from the store. The operation is applied via Raft log.
// // This method must be called on the leader node.
func (kv *KVReplicator) Delete(key string) error {
	if !kv.raftManager.IsLeader() {
		leaderAddr := kv.raftManager.Leader()
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

	// Apply the command through the Raft manager
	applyFuture := kv.raftManager.ApplyCommand(data, kv.config.ApplyTimeout)
	if err := applyFuture.Error(); err != nil {
		return fmt.Errorf("failed to apply %s command via Raft: %w", cmd.Op, err)
	}

	// Check the response from the FSM application
	response := applyFuture.Response()
	if err, ok := response.(error); ok {
		return fmt.Errorf("%s command failed to apply on FSM: %w", cmd.Op, err)
	}

	// Log success after confirming FSM application
	kv.logger.Printf("%s successful: Key=%s", cmd.Op, key)
	return nil
}

// IsLeader checks if the current node is the Raft leader by consulting the raftManager.
func (kv *KVReplicator) IsLeader() bool {
	if kv.raftManager == nil {
		return false // Not initialized
	}
	return kv.raftManager.IsLeader()
}

// Leader returns the Raft address of the current leader by consulting the raftManager.
// Returns empty string if there is no current leader.
func (kv *KVReplicator) Leader() raft.ServerAddress {
	if kv.raftManager == nil {
		return "" // Not initialized
	}
	return kv.raftManager.Leader()
}

// Shutdown gracefully shuts down the KVReplicator, including the Raft manager and KV store.
func (kv *KVReplicator) Shutdown() error {
	kv.logger.Println("Shutting down KVReplicator...")
	var raftErr, kvErr error

	// Shutdown Raft manager first
	if kv.raftManager != nil {
		raftErr = kv.raftManager.Shutdown()
		if raftErr != nil {
			kv.logger.Printf("ERROR: failed to shut down Raft manager: %v", raftErr)
		}
	} else {
		kv.logger.Println("Raft manager not initialized.")
	}

	// Close KV store
	if kv.kvStore != nil {
		kvErr = kv.kvStore.Close()
		if kvErr != nil {
			kv.logger.Printf("ERROR: failed to close KV Store: %v", kvErr)
		}
	} else {
		kv.logger.Println("KV Store not initialized.")
	}

	// Return the first error encountered, if any
	if raftErr != nil {
		return fmt.Errorf("KVReplicator shutdown encountered errors: Raft manager: %w", raftErr)
	}
	if kvErr != nil {
		return fmt.Errorf("KVReplicator shutdown encountered errors: KV Store: %w", kvErr)
	}

	kv.logger.Println("KVReplicator shut down successfully.")
	return nil
}

// Stats returns basic stats from the Raft node by consulting the raftManager.
func (kv *KVReplicator) Stats() map[string]string {
	if kv.raftManager == nil {
		return map[string]string{"error": "Raft manager not initialized"}
	}
	return kv.raftManager.Stats()
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

	// Delegate the actual Raft operation to the raftManager
	// The raftManager's AddVoter method handles checking if the server already exists internally.
	return kv.raftManager.AddVoter(serverID, serverAddress)
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

	// Delegate the actual Raft operation to the raftManager
	return kv.raftManager.RemoveServer(serverID)
}
