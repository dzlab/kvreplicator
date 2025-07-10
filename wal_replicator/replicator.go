package wal_replicator

import (
	"log"
	"net/rpc"
	"sync"
	"time"
)

// Replicator manages the push-based WAL replication from primary to followers.
// PrimaryChecker defines the interface for checking primary status and getting primary address.
type PrimaryChecker interface {
	IsPrimary() bool
	GetPrimary() string
}

// NodeRegistry defines the interface for getting active nodes from the registry.
type NodeRegistry interface {
	GetActiveNodes() map[string]string
}

// WALSource defines the interface for retrieving WAL updates and sequence numbers.
type WALSource interface {
	GetLatestSequenceNumber() (uint64, error)
	GetUpdatesSince(uint64) ([]WALUpdate, error)
}

// Replicator manages the push-based WAL replication from primary to followers.
type Replicator struct {
	logger         *log.Logger
	nodeID         string
	primaryChecker PrimaryChecker // Interface for primary status and address
	nodeRegistry   NodeRegistry   // Interface for active node management
	walSource      WALSource      // Interface for WAL data access

	// Replication state
	followerConnections map[string]*rpc.Client // Map of follower NodeID to RPC client connection
	followerSequence    map[string]uint64      // Map of follower NodeID to their last replicated sequence number

	mu sync.RWMutex // Mutex for followerConnections and followerSequence

	// Control channels
	stopChan         chan struct{}
	triggerReconcile chan struct{}
}

// NewReplicator creates and initializes a new Replicator instance.
func NewReplicator(
	logger *log.Logger,
	nodeID string,
	primaryChecker PrimaryChecker,
	nodeRegistry NodeRegistry,
	walSource WALSource,
) *Replicator {
	return &Replicator{
		logger:              logger,
		nodeID:              nodeID,
		primaryChecker:      primaryChecker,
		nodeRegistry:        nodeRegistry,
		walSource:           walSource,
		followerConnections: make(map[string]*rpc.Client),
		followerSequence:    make(map[string]uint64),
		stopChan:            make(chan struct{}),
		triggerReconcile:    make(chan struct{}, 1), // Buffered channel to avoid blocking sender
	}
}

// Start initiates the replication process.
func (r *Replicator) Start() {
	r.logger.Println("Starting WAL Replicator...")
	go r.replicationLoop()
	r.logger.Println("WAL Replicator started.")
}

// Stop gracefully shuts down the Replicator.
func (r *Replicator) Stop() {
	r.logger.Println("Stopping WAL Replicator...")
	close(r.stopChan)
	r.disconnectAllFollowers()
	r.logger.Println("WAL Replicator stopped.")
}

// TriggerReconciliation can be called to signal the replicator to immediately reconcile its follower connections.
func (r *Replicator) TriggerReconciliation() {
	select {
	case r.triggerReconcile <- struct{}{}:
		r.logger.Println("Replicator reconciliation triggered.")
	default:
		// Already a trigger pending, no need to add another.
		r.logger.Println("Replicator reconciliation already pending, skipping trigger.")
	}
}

// replicationLoop is the main loop for the Replicator.
// It continuously checks primary status and manages replication.
func (r *Replicator) replicationLoop() {
	ticker := time.NewTicker(5 * time.Second) // Periodically reconcile and push updates
	defer ticker.Stop()

	for {
		select {
		case <-r.stopChan:
			r.logger.Println("Replication loop received stop signal.")
			return
		case <-ticker.C:
			r.reconcileFollowerConnections()
			r.pushWALUpdates()
		case <-r.triggerReconcile:
			r.logger.Println("Replication loop received reconciliation trigger.")
			r.reconcileFollowerConnections()
			r.pushWALUpdates()
		}
	}
}

// reconcileFollowerConnections establishes/removes connections based on active nodes and primary status.
func (r *Replicator) reconcileFollowerConnections() {
	if !r.primaryChecker.IsPrimary() {
		r.logger.Println("Not primary, disconnecting all followers.")
		r.disconnectAllFollowers()
		return
	}

	r.logger.Println("Reconciling follower connections as primary...")
	activeNodes := r.nodeRegistry.GetActiveNodes()

	// Identify nodes that are no longer active followers
	r.mu.Lock()
	for followerID, conn := range r.followerConnections {
		if _, exists := activeNodes[followerID]; !exists || followerID == r.nodeID {
			r.logger.Printf("Disconnecting inactive or self-node follower: %s", followerID)
			conn.Close()
			delete(r.followerConnections, followerID)
			delete(r.followerSequence, followerID)
		}
	}

	// Establish connections to new active followers
	for nodeID, addr := range activeNodes {
		if nodeID == r.nodeID {
			continue // Don't connect to self
		}

		if _, connected := r.followerConnections[nodeID]; !connected {
			r.logger.Printf("Attempting to connect to new follower %s at %s...", nodeID, addr)
			client, err := rpc.DialHTTP("tcp", addr) // Assuming RPC over HTTP
			if err != nil {
				r.logger.Printf("ERROR: Failed to connect to follower %s at %s: %v", nodeID, addr, err)
				continue
			}
			r.followerConnections[nodeID] = client
			// Initialize follower sequence number to 0, it will be updated on first sync
			r.followerSequence[nodeID] = 0
			r.logger.Printf("Successfully connected to follower %s at %s.", nodeID, addr)
		}
	}
	r.mu.Unlock()
}

// disconnectAllFollowers closes all active follower RPC connections.
func (r *Replicator) disconnectAllFollowers() {
	r.mu.Lock()
	defer r.mu.Unlock()

	for followerID, conn := range r.followerConnections {
		r.logger.Printf("Closing connection to follower: %s", followerID)
		if err := conn.Close(); err != nil {
			r.logger.Printf("ERROR: Failed to close connection to follower %s: %v", followerID, err)
		}
		delete(r.followerConnections, followerID)
		delete(r.followerSequence, followerID)
	}
	r.logger.Println("All follower connections closed.")
}

// pushWALUpdates attempts to push WAL entries to all connected followers.
func (r *Replicator) pushWALUpdates() {
	if !r.primaryChecker.IsPrimary() {
		return // Only primary pushes updates
	}

	r.mu.RLock()
	defer r.mu.RUnlock()

	latestSeqNum, err := r.walSource.GetLatestSequenceNumber()
	if err != nil {
		r.logger.Printf("ERROR: Failed to get latest sequence number from DB: %v", err)
		return
	}

	for followerID, conn := range r.followerConnections {
		go func(id string, client *rpc.Client) {
			lastSyncedSeq := r.followerSequence[id]
			if latestSeqNum <= lastSyncedSeq {
				// r.logger.Printf("Follower %s is up-to-date (seq: %d)", id, lastSyncedSeq)
				return
			}

			r.logger.Printf("Pushing updates to follower %s from sequence %d to %d", id, lastSyncedSeq, latestSeqNum)
			updates, err := r.walSource.GetUpdatesSince(lastSyncedSeq)
			if err != nil {
				r.logger.Printf("ERROR: Failed to get WAL updates for follower %s from DB: %v", id, err)
				return
			}

			if len(updates) == 0 {
				// This can happen if latestSeqNum was from a read, but no actual new writes occurred since lastSyncedSeq.
				// Or if updates were filtered out (e.g., internal-only ops).
				// r.logger.Printf("No new WAL updates for follower %s since sequence %d.", id, lastSyncedSeq)
				return
			}

			// Define the RPC request and response types if needed for actual RPC calls
			// For now, assume a simple call for placeholder.
			var reply string // Or a more structured reply if needed

			// Example: Call a remote RPC method on the follower
			// client.Call("FollowerService.ApplyWALUpdates", updates, &reply)
			// Replace with actual RPC call once follower RPC service is defined.
			r.logger.Printf("INFO: Simulating push of %d updates to follower %s. (Need to implement actual RPC call).", len(updates), id)
			_ = reply // Placeholder to avoid unused variable warning

			// Update the last synced sequence number for this follower
			r.mu.Lock()
			r.followerSequence[id] = latestSeqNum
			r.mu.Unlock()
			r.logger.Printf("Successfully simulated push of updates to follower %s. New synced sequence: %d", id, latestSeqNum)

		}(followerID, conn)
	}
}
