package wal_replicator

import (
	"fmt"
	"log"
	"net/rpc"
	"sort"
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

// ApplyWALUpdatesArgs defines the arguments for the RPC call to apply WAL updates.
type ApplyWALUpdatesArgs struct {
	Updates []WALUpdate
}

// ApplyWALUpdatesReply defines the reply for the RPC call to apply WAL updates.
type ApplyWALUpdatesReply struct {
	Success        bool
	Message        string
	ReceivedSeqNum uint64 // The highest sequence number successfully *received* by the follower.
	AppliedSeqNum  uint64 // The highest sequence number successfully *applied* to the local state.
}

// Replicator manages the push-based WAL replication from primary to followers.
type Replicator struct {
	logger         *log.Logger
	nodeID         string
	primaryChecker PrimaryChecker // Interface for primary status and address
	nodeRegistry   NodeRegistry   // Interface for active node management
	walSource      WALSource      // Interface for WAL data access

	// Replication state
	followerConnections  map[string]*rpc.Client // Map of follower NodeID to RPC client connection
	followerReceivedSeq  map[string]uint64      // Map of follower NodeID to their last *received* sequence number
	followerAppliedSeq   map[string]uint64      // Map of follower NodeID to their last *applied* sequence number
	committedSequenceNum uint64                 // The highest sequence number committed by a quorum
	mu                   sync.RWMutex           // Mutex for followerConnections, followerReceivedSeq, followerAppliedSeq, and committedSequenceNum

	// Configuration for quorum (example, could be moved to WALConfig)
	replicationQuorum int // Number of followers that must acknowledge an update for it to be considered committed

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
		logger:               logger,
		nodeID:               nodeID,
		primaryChecker:       primaryChecker,
		nodeRegistry:         nodeRegistry,
		walSource:            walSource,
		followerConnections:  make(map[string]*rpc.Client),
		followerReceivedSeq:  make(map[string]uint64), // Initialize new map
		followerAppliedSeq:   make(map[string]uint64), // Initialize new map
		committedSequenceNum: 0,                       // Initialize committed sequence number
		replicationQuorum:    1,                       // Default to 1 for now (leader + 0 followers for 1-node quorum, adjust for N+1)
		stopChan:             make(chan struct{}),
		triggerReconcile:     make(chan struct{}, 1), // Buffered channel to avoid blocking sender
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

// GetCommittedSequenceNumber returns the highest sequence number that has been committed by a quorum.
func (r *Replicator) GetCommittedSequenceNumber() uint64 {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.committedSequenceNum
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
			delete(r.followerReceivedSeq, followerID) // Clean up new map
			delete(r.followerAppliedSeq, followerID)  // Clean up new map
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
			// Initialize follower sequence numbers to 0, they will be updated on first sync
			r.followerReceivedSeq[nodeID] = 0
			r.followerAppliedSeq[nodeID] = 0
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
		delete(r.followerReceivedSeq, followerID) // Clean up new map
		delete(r.followerAppliedSeq, followerID)  // Clean up new map
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

	// Use a channel to collect RPC replies for quorum tracking
	type rpcReply struct {
		followerID   string
		receivedSeq  uint64
		appliedSeq   uint64
		success      bool
		errorMessage string
	}
	replyChan := make(chan rpcReply, len(r.followerConnections))

	// Track the sequence number to which each follower is currently synced.
	// We'll use followerAppliedSeq for this check.
	// Note: This loop uses RLock, so maps are safe to read.
	for followerID, conn := range r.followerConnections {
		go func(id string, client *rpc.Client) {
			lastAppliedSeq := r.followerAppliedSeq[id] // Use followerAppliedSeq for checking "up-to-date"
			if latestSeqNum <= lastAppliedSeq {
				// Follower is up-to-date with what's committed on the primary's side.
				replyChan <- rpcReply{followerID: id, receivedSeq: latestSeqNum, appliedSeq: lastAppliedSeq, success: true}
				return
			}

			r.logger.Printf("Pushing updates to follower %s from sequence %d to %d (latest primary: %d)", id, lastAppliedSeq, latestSeqNum, latestSeqNum)
			updates, err := r.walSource.GetUpdatesSince(lastAppliedSeq) // Fetch updates since last *applied* sequence
			if err != nil {
				r.logger.Printf("ERROR: Failed to get WAL updates for follower %s from DB: %v", id, err)
				replyChan <- rpcReply{followerID: id, success: false, errorMessage: fmt.Sprintf("Failed to get WAL updates: %v", err)}
				return
			}

			if len(updates) == 0 {
				// This can happen if latestSeqNum was from a read, but no actual new writes occurred since lastAppliedSeq,
				// or if updates were filtered out (e.g., internal-only ops).
				// If we fetched from `lastAppliedSeq` and got nothing, it means the follower is conceptually caught up
				// to the primary's last acknowledged state, even if primary has slightly newer ops not yet applied/committed.
				// However, if `latestSeqNum > lastAppliedSeq` and `updates` is empty, it might mean the primary's
				// `latestSeqNum` is ahead due to internal writes that aren't external `WALUpdate`s.
				// For now, we consider them up-to-date with relevant changes.
				replyChan <- rpcReply{followerID: id, receivedSeq: latestSeqNum, appliedSeq: latestSeqNum, success: true}
				return
			}

			args := ApplyWALUpdatesArgs{Updates: updates}
			var reply ApplyWALUpdatesReply

			// Call the remote RPC method on the follower to apply WAL updates.
			// The "WALReplicationServer" string assumes that the WALReplicationServer
			// instance on the follower node will register itself as an RPC service.
			err = client.Call("WALReplicationServer.ApplyWALUpdates", args, &reply)
			if err != nil {
				r.logger.Printf("ERROR: RPC call to follower %s failed: %v", id, err)
				replyChan <- rpcReply{followerID: id, success: false, errorMessage: fmt.Sprintf("RPC failed: %v", err)}
				// Consider marking this follower as needing reconciliation or retrying.
				return
			}

			if !reply.Success {
				r.logger.Printf("ERROR: Follower %s failed to apply WAL updates: %s", id, reply.Message)
				replyChan <- rpcReply{followerID: id, success: false, errorMessage: reply.Message}
				return
			}

			// Report success with the received and applied sequence numbers from the follower
			replyChan <- rpcReply{
				followerID:  id,
				receivedSeq: reply.ReceivedSeqNum,
				appliedSeq:  reply.AppliedSeqNum,
				success:     true,
			}
			r.logger.Printf("Successfully pushed %d updates to follower %s. Follower reported Received: %d, Applied: %d",
				len(updates), id, reply.ReceivedSeqNum, reply.AppliedSeqNum)

		}(followerID, conn)
	}

	// Wait for replies from all RPC calls and update states

	// We need to know how many actual replicas are there, including primary if it's part of quorum.
	// A common quorum is N/2 + 1, where N is the total number of nodes (including primary).
	// For this implementation, let's define replicationQuorum as the number of *followers*
	// that must acknowledge an update. The primary implicitly acknowledges its own write.
	// So, if replicationQuorum is 1, it means the primary + 1 follower.
	// If replicationQuorum is 0, it means primary only (no followers required for commit).
	numConnectedFollowers := len(r.followerConnections)
	requiredFollowerAcks := r.replicationQuorum

	// Calculate the actual quorum needed, considering the primary's implicit acknowledgment.
	// An update is committed when the primary has written it locally, PLUS
	// requiredFollowerAcks followers have applied it.
	// This means total nodes with applied updates must be at least (1 + requiredFollowerAcks).

	// If there are no followers, the leader commits immediately (its own write is enough).
	if numConnectedFollowers == 0 {
		r.mu.Lock()
		if latestSeqNum > r.committedSequenceNum {
			r.committedSequenceNum = latestSeqNum
			r.logger.Printf("No followers connected, updates up to %d are immediately considered committed (leader-only).", latestSeqNum)
		}
		r.mu.Unlock()
		return
	}

	// Wait for replies from all RPC calls and update states
	// For quorum, we need to count how many followers have applied up to a certain sequence number.
	// The primary itself also contributes to the quorum (implicitly applies its own writes).
	// A more robust solution would collect all replies, find the Nth highest applied sequence,
	// and set that as the committed sequence. For simplicity, we'll try to push `latestSeqNum`
	// and see how many manage to apply it.

	// Collect replies for quorum analysis
	replies := make([]rpcReply, 0, numConnectedFollowers)
	for i := 0; i < numConnectedFollowers; i++ {
		select {
		case reply := <-replyChan:
			replies = append(replies, reply)
			r.mu.Lock()
			if reply.success {
				if reply.receivedSeq > r.followerReceivedSeq[reply.followerID] {
					r.followerReceivedSeq[reply.followerID] = reply.receivedSeq
				}
				if reply.appliedSeq > r.followerAppliedSeq[reply.followerID] {
					r.followerAppliedSeq[reply.followerID] = reply.appliedSeq
				}
			} else {
				r.logger.Printf("Follower %s reported error during push: %s", reply.followerID, reply.errorMessage)
				// Consider marking this follower as needing reconciliation or retrying.
			}
			r.mu.Unlock()
		case <-time.After(5 * time.Second): // Timeout for RPC replies from a single push
			r.logger.Printf("Timeout waiting for some RPC replies. Some acknowledgments may be missed for this push cycle.")
			// Break from the loop after timeout to process replies received so far.
			goto EndQuorumCollection
		}
	}

EndQuorumCollection:
	// Determine the highest sequence number committed by a quorum.
	// The primary is assumed to have applied `latestSeqNum` if it originated the operation.
	// This function currently processes pushes, not individual client operations,
	// so `latestSeqNum` is the highest sequence number currently on the primary.
	// We need `requiredFollowerAcks` more acknowledgments beyond the primary's.

	appliedByQuorum := make([]uint64, 0, len(replies)+1) // +1 for the primary itself
	appliedByQuorum = append(appliedByQuorum, latestSeqNum) // Primary's local application

	r.mu.RLock()
	for _, reply := range replies {
		if reply.success {
			appliedByQuorum = append(appliedByQuorum, reply.appliedSeq)
		}
	}
	r.mu.RUnlock()

	// Sort applied sequence numbers in descending order.
	sort.Slice(appliedByQuorum, func(i, j int) bool {
		return appliedByQuorum[i] > appliedByQuorum[j]
	})

	// The (requiredFollowerAcks + 1)th element (0-indexed) in the sorted list is the committed sequence number.
	// This represents the sequence number that `requiredFollowerAcks` followers PLUS the primary have applied.
	// If there are not enough nodes, it means quorum is not met for `latestSeqNum`.
	if len(appliedByQuorum) >= (requiredFollowerAcks + 1) {
		r.mu.Lock()
		// Only advance committedSequenceNum if the new value is higher.
		// This handles cases where a smaller set of replicas acknowledge a higher sequence first.
		potentialCommittedSeq := appliedByQuorum[requiredFollowerAcks]
		if potentialCommittedSeq > r.committedSequenceNum {
			r.committedSequenceNum = potentialCommittedSeq
			r.logger.Printf("Quorum achieved! Committed sequence number advanced to %d (applied by %d nodes including primary, %d required beyond primary).",
				r.committedSequenceNum, len(appliedByQuorum), requiredFollowerAcks)
		} else {
			r.logger.Printf("Quorum achieved for previous sequence numbers, but not advancing committed sequence number (%d current, %d potential).", r.committedSequenceNum, potentialCommittedSeq)
		}
		r.mu.Unlock()
	} else {
		r.logger.Printf("Quorum NOT achieved for latest updates (Applied by %d nodes including primary, %d followers required beyond primary for commit).",
			len(appliedByQuorum), requiredFollowerAcks)
		// The primary's committedSequenceNum will not advance.
	}
}
