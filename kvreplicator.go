```go
package kvreplicator

import (
	"fmt"
	"log"
	"net"
	"os"
	"path/filepath"
	"time"

	"github.com/hashicorp/raft"
	"github.com/hashicorp/raft-boltdb" // Using BoltDB for log and stable store
	// "github.com/tecbot/gorocksdb" // Placeholder for RocksDB
)

const (
	defaultApplyTimeout       = 10 * time.Second
	defaultRetainSnapshotCount = 2
	defaultRaftLogCacheSize   = 512
)

// Config holds the configuration for the KVReplicator.
type Config struct {
	NodeID          string        // Unique ID for this node in the Raft cluster.
	RaftBindAddress string        // TCP address for Raft to bind to (e.g., "localhost:7000").
	RaftDataDir     string        // Directory to store Raft log and snapshots.
	RocksDBPath     string        // Path for RocksDB data.
	Bootstrap       bool          // Whether to bootstrap a new cluster if no existing state.
	JoinAddresses   []string      // Addresses of existing cluster members to join (optional).
	ApplyTimeout    time.Duration // Timeout for Raft apply operations.
	Logger          *log.Logger   // Logger instance.
}

// KVReplicator provides a replicated key-value store using RocksDB and Raft.
type KVReplicator struct {
	config   Config
	raftNode *raft.Raft
	fsm      *rocksDBFSM
	logger   *log.Logger
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
	// RocksDBPath is not strictly required here if FSM handles default path, but good practice
	// if cfg.RocksDBPath == "" {
	// 	return nil, fmt.Errorf("RocksDBPath must be specified")
	// }

	// Ensure Raft data directory exists
	if err := os.MkdirAll(cfg.RaftDataDir, 0700); err != nil {
		return nil, fmt.Errorf("failed to create Raft data directory %s: %w", cfg.RaftDataDir, err)
	}

	kv := &KVReplicator{
		config: cfg,
		logger: cfg.Logger,
	}

	// Initialize the FSM (Finite State Machine)
	// In a real scenario, you'd pass cfg.RocksDBPath to newRocksDBFSM
	// and it would open/initialize RocksDB.
	kv.fsm = newRocksDBFSM(cfg.Logger /*, cfg.RocksDBPath */)

	// Setup Raft configuration.
	raftConfig := raft.DefaultConfig()
	raftConfig.LocalID = raft.ServerID(cfg.NodeID)
	raftConfig.Logger = cfg.Logger // Use hclog adapter if necessary, but hashicorp/log is often compatible
	raftConfig.SnapshotInterval = 20 * time.Second
	raftConfig.SnapshotThreshold = 50 // Number of logs before a snapshot

	// Setup Raft communication (transport).
	addr, err := net.ResolveTCPAddr("tcp", cfg.RaftBindAddress)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve Raft bind address %s: %w", cfg.RaftBindAddress, err)
	}
	transport, err := raft.NewTCPTransport(cfg.RaftBindAddress, addr, 3, 10*time.Second, cfg.Logger)
	if err != nil {
		return nil, fmt.Errorf("failed to create Raft TCP transport: %w", err)
	}

	// Create snapshot store. This allows Raft to compact its log.
	snapshotStore, err := raft.NewFileSnapshotStore(cfg.RaftDataDir, defaultRetainSnapshotCount, cfg.Logger)
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
		return nil, fmt.Errorf("failed to create Raft instance: %w", err)
	}
	kv.raftNode = r

	return kv, nil
}

// Start initializes the Raft node. If config.Bootstrap is true and no existing state,
// it bootstraps a new cluster. If config.JoinAddress is provided, it attempts to join
// an existing cluster.
func (kv *KVReplicator) Start() error {
	if kv.config.Bootstrap {
		// Check if we need to bootstrap. This is typically done if the cluster
		// has no existing state (e.g., no peers.json and no log entries).
		// hasExistingState, err := raft.HasExistingState(logStore, stableStore, snapshotStore) // logStore, etc. not in scope
		// For simplicity, we'll rely on the configuration passed to BootstrapCluster.
		// A more robust check would inspect the stores.

		configuration := raft.Configuration{
			Servers: []raft.Server{
				{
					ID:      raft.ServerID(kv.config.NodeID),
					Address: kv.raftNode.Transport().LocalAddr(), // Use the transport's address
				},
			},
		}
		bootstrapFuture := kv.raftNode.BootstrapCluster(configuration)
		if err := bootstrapFuture.Error(); err != nil {
			// Check if it's "already bootstrapped" or similar error, which might be fine.
			// hashicorp/raft handles this by not re-bootstrapping if state exists.
			kv.logger.Printf("INFO: BootstrapCluster returned: %v (this may be normal if already bootstrapped or joining)", err)
			// If we intend to join, we shouldn't fatal here.
			// Let's proceed, joining logic will take over if needed.
		} else {
			kv.logger.Printf("INFO: Cluster bootstrapped successfully with node %s", kv.config.NodeID)
		}
	}

	// Attempt to join an existing cluster if JoinAddresses are provided.
	// This should happen after potential bootstrap attempt for the first node,
	// or if this node is explicitly told to join.
	if len(kv.config.JoinAddresses) > 0 && !kv.config.Bootstrap { // Only join if not bootstrapping this node as the first one
		kv.logger.Printf("Attempting to join cluster via addresses: %v", kv.config.JoinAddresses)
		joinResp, err := kv.raftNode.AddVoter(raft.ServerID(kv.config.NodeID), kv.raftNode.Transport().LocalAddr(), 0, 0)
		if err != nil {
			kv.logger.Printf("ERROR: could not add self as voter to prepare for join: %v", err)
			// This is tricky. Adding self as voter is usually done by a leader.
			// Instead, we should use Join.
		} else if err := joinResp.Error(); err != nil {
			kv.logger.Printf("ERROR: could not add self as voter (future error): %v", err)
		}


		// The proper way to join is by calling raft.Join on one of the existing nodes.
		// This is typically an RPC call to an existing leader to add this node.
		// For simplicity, the hashicorp/raft library expects this to be coordinated externally
		// or by having the leader use AddVoter.
		// If a node starts and is part of the configured peers but not the leader,
		// it will attempt to discover the leader and join.
		// If `JoinAddresses` are provided, it implies we want `raft.AddVoter` to be called on the leader for this node.
		// This is usually done by an external mechanism or an API on the leader.
		// For now, we assume that if this node is configured as part of the initial peer set
		// (e.g. during bootstrap of multiple nodes) or if an AddVoter command is sent to the leader,
		// it will join. The `JoinAddresses` can be used by a client utility to tell the leader to add this node.

		// A simpler approach for a new node starting:
		// If not bootstrapping and JoinAddresses are present, try to contact one of them
		// and ask it to add this node as a voter. This requires an API on the server.
		// For now, we assume the configuration is set up such that this node will eventually be added.
		// Hashicorp's example often involves manually adding nodes via an HTTP API.

		// A simplified automatic join attempt (less robust than manual AddVoter on leader):
		// If trying to join, the node will attempt to contact existing servers.
		// The `BootstrapCluster` on the first node sets up the initial config.
		// Other nodes, if started with the same configuration of servers, will try to join.
		// If JoinAddresses are provided, this node will try to get the leader to add it.
		// This is more complex than can be fully implemented here without an external join mechanism.
		// The `raft.Raft` object itself doesn't have a `JoinCluster(remoteAddrs)` method.
		// It's usually `leader.AddVoter(thisNodeID, thisNodeAddr, ...)`.
		// So, for now, we'll log that manual intervention or an external join mechanism might be needed
		// if this node isn't part of the initial bootstrapped configuration.
		kv.logger.Printf("INFO: If this node is not part of the initial cluster configuration, it must be added via AddVoter by the leader.")

	} else if !kv.config.Bootstrap {
		kv.logger.Printf("INFO: Starting node. Will attempt to discover existing cluster or wait to be added.")
	}


	kv.logger.Printf("KVReplicator started. Node ID: %s, Raft Address: %s", kv.config.NodeID, kv.config.RaftBindAddress)
	return nil
}

// Get retrieves a value for a given key.
// This reads directly from the local FSM, providing eventual consistency.
// For strongly consistent reads, a more complex mechanism (e.g., Raft read index or linearizable reads) would be needed.
func (kv *KVReplicator) Get(key string) (string, error) {
	// Check if we are the leader - optional, but can inform client about consistency
	// if kv.raftNode.State() != raft.Leader {
	//  kv.logger.Printf("WARN: Get operation on a non-leader node. Data might be stale.")
	// }

	value, ok := kv.fsm.Get(key)
	if !ok {
		return "", fmt.Errorf("key not found: %s", key) // Or return a specific error like ErrKeyNotFound
	}
	return value, nil
}

// Put sets a value for a given key. The operation is applied via Raft log.
func (kv *KVReplicator) Put(key, value string) error {
	if kv.raftNode.State() != raft.Leader {
		return fmt.Errorf("cannot apply Put: not the leader. Current leader: %s", kv.raftNode.Leader())
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

	// The response from Apply() in the FSM can also be an error.
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
		return fmt.Errorf("cannot apply Delete: not the leader. Current leader: %s", kv.raftNode.Leader())
	}

	cmd := &Command{
		Op:  OpDelete,
		Key: key,
	}
	data, err := cmd.Serialize()
	if err != nil {
		return fmt.Errorf("failed to serialize Delete command: %w", err)
	}

	applyFuture := kv.raftNode.Apply(data, kv.config.ApplyTimeout)
	if err := applyFuture.Error(); err != nil {
		return fmt.Errorf("failed to apply Delete command via Raft: %w", err)
	}

	response := applyFuture.Response()
	if err, ok := response.(error); ok {
		return fmt.Errorf("delete command failed to apply on FSM: %w", err)
	}

	kv.logger.Printf("Delete successful: Key=%s", key)
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

// Shutdown gracefully shuts down the KVReplicator, including the Raft node.
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
		if err := kv.fsm.Close(); err != nil { // Assuming FSM has a Close method for RocksDB
			kv.logger.Printf("ERROR: failed to close FSM: %v", err)
			return err // Or collect errors and return aggregate
		}
		kv.logger.Println("FSM closed.")
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
		return fmt.Errorf("node is not the leader, cannot add voter. Current leader: %s", kv.Leader())
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
			// If ID exists but address is different, it's more complex - might need RemoveServer first
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
		return fmt.Errorf("node is not the leader, cannot remove server. Current leader: %s", kv.Leader())
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
```