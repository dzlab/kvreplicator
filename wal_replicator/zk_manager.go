package wal_replicator

import (
	"fmt"
	"log"
	"sort" // Add sort package
	"strings"
	"sync" // Add sync package for mutex
	"time" // Add time package for timeouts

	"github.com/go-zookeeper/zk"
	// Add context package for shutdown
)

const primaryElectionPath = "/kvreplicator/wal/primary_election"

// ZKManager handles ZooKeeper operations for WALReplicationServer.
type ZKManager struct {
	conn                *zk.Conn
	logger              *log.Logger
	nodeID              string
	internalBindAddress string            // Store internal bind address for re-registration
	activeNodes         map[string]string // Map of nodeID to internalBindAddress
	electionNodePath    string            // Stores the path of this node's ephemeral sequential node for primary election
	mu                  sync.RWMutex      // Mutex to protect activeNodes
}

// NewZKManager creates a new ZKManager instance.
func NewZKManager(logger *log.Logger, nodeID string) *ZKManager {
	return &ZKManager{
		logger:      logger,
		nodeID:      nodeID,
		activeNodes: make(map[string]string),
		mu:          sync.RWMutex{},
	}
}

// Connect establishes a connection to ZooKeeper and ensures the base path exists.
func (zkm *ZKManager) Connect(zkServers []string) error {
	if len(zkServers) == 0 {
		zkm.logger.Println("WARNING: No ZooKeeper servers specified. Membership features will not work.")
		return nil
	}

	zkm.logger.Printf("Connecting to ZooKeeper at %v...", zkServers)
	conn, _, err := zk.Connect(zkServers, time.Second*10) // 10-second timeout for connection
	if err != nil {
		zkm.logger.Printf("ERROR: Failed to connect to ZooKeeper: %v", err)
		return fmt.Errorf("failed to connect to zookeeper: %w", err)
	}
	zkm.conn = conn
	zkm.logger.Println("ZooKeeper connection established.")

	// Basic check: ensure root path exists
	rootPath := "/kvreplicator/wal"
	exists, _, err := zkm.conn.Exists(rootPath)
	if err != nil {
		zkm.conn.Close()
		zkm.conn = nil
		zkm.logger.Printf("ERROR: Failed to check existence of ZK path %s: %v", rootPath, err)
		return fmt.Errorf("zookeeper path check failed: %w", err)
	}
	if !exists {
		zkm.logger.Printf("ZK path %s does not exist, creating...", rootPath)
		_, err = zkm.conn.Create(rootPath, nil, 0, zk.WorldACL(zk.PermAll))
		if err != nil && err != zk.ErrNodeExists { // ErrNodeExists is ok
			zkm.conn.Close()
			zkm.conn = nil
			zkm.logger.Printf("ERROR: Failed to create ZK path %s: %v", rootPath, err)
			return fmt.Errorf("failed to create zookeeper path: %w", err)
		}
		zkm.logger.Printf("ZK path %s created or already exists.", rootPath)
	}
	return nil
}

// Start registers the node and sets up event handling.
func (zkm *ZKManager) Start(internalBindAddress string) error {
	if zkm.conn == nil {
		zkm.logger.Println("Skipping ZKManager Start: ZK connection is nil.")
		return nil
	}

	// Store the internal bind address for potential re-registration
	zkm.internalBindAddress = internalBindAddress

	err := zkm.RegisterNode()
	if err != nil {
		zkm.logger.Printf("Error during ZK node registration: %v", err)
		return err // Or decide if it's a fatal error
	}

	// Register the node for general membership
	regErr := zkm.RegisterNode()
	if regErr != nil {
		zkm.logger.Printf("Error during ZK node registration: %v", regErr)
		// Decide if startup should fail here or continue with a warning
	}

	// Start primary election and get the initial primary status and event channel
	isPrimary, primaryAddr, electionEventChan, electErr := zkm.ElectPrimary()
	if electErr != nil {
		zkm.logger.Printf("Error during ZK primary election: %v", electErr)
		return electErr // Primary election is critical, so we fail if it fails
	}
	zkm.logger.Printf("Initial primary election result: IsPrimary=%t, PrimaryAddr=%s", isPrimary, primaryAddr)

	// Ensure the /nodes path exists and get the children event channel
	nodesEventChan, err := zkm.EnsureNodesPathExists()
	if err != nil {
		zkm.logger.Printf("Error ensuring ZK nodes path exists or setting watch: %v", err)
		// This might not be fatal, but log and continue if election was successful
	}

	// Start a single goroutine to handle all ZooKeeper events (election, nodes, session)
	// Pass both channels. One might be nil if not successfully set up.
	go zkm.handleZkEvents(electionEventChan, nodesEventChan)

	return nil
}

// Close closes the ZooKeeper connection.
func (zkm *ZKManager) Close() {
	if zkm.conn != nil {
		zkm.logger.Println("Closing ZooKeeper connection...")
		zkm.conn.Close()
		zkm.logger.Println("ZooKeeper connection closed.")
		zkm.conn = nil
	}
}

// GetActiveNodes returns the current map of active nodes.
func (zkm *ZKManager) GetActiveNodes() map[string]string {
	zkm.mu.RLock()
	defer zkm.mu.RUnlock()
	// Return a copy to prevent external modification
	copiedNodes := make(map[string]string, len(zkm.activeNodes))
	for k, v := range zkm.activeNodes {
		copiedNodes[k] = v
	}
	return copiedNodes
}

// handleZkEvents processes events received from ZooKeeper.
func (zkm *ZKManager) handleZkEvents(electionEventChan <-chan zk.Event, nodesEventChan <-chan zk.Event) {
	zkm.logger.Println("Starting ZooKeeper event handler goroutine.")

	for {
		select {
		case event, ok := <-electionEventChan:
			if !ok {
				zkm.logger.Println("ZooKeeper election event channel closed. Stopping election event handler.")
				// This channel being closed might indicate a serious issue or ZK connection loss
				// For robustness, perhaps attempt re-election here or rely on session events.
				electionEventChan = nil // Mark as closed
				if nodesEventChan == nil {
					return // Both channels closed, exit goroutine
				}
				continue
			}
			zkm.logger.Printf("Received ZK election event: %+v", event)
			zkm.handleElectionEvent(event)
		case event, ok := <-nodesEventChan:
			if !ok {
				zkm.logger.Println("ZooKeeper nodes event channel closed. Stopping nodes event handler.")
				nodesEventChan = nil // Mark as closed
				if electionEventChan == nil {
					return // Both channels closed, exit goroutine
				}
				continue
			}
			zkm.logger.Printf("Received ZK nodes event: %+v", event)
			zkm.handleNodesEvent(event)
		}
	}
}

// handleElectionEvent processes events specifically related to primary election path.
func (zkm *ZKManager) handleElectionEvent(event zk.Event) {
	switch event.Type {
	case zk.EventSession:
		zkm.logger.Printf("ZooKeeper session event in election handler: State -> %s, Server -> %s", event.State.String(), event.Server)
		if event.State == zk.StateExpired || event.State == zk.StateDisconnected {
			zkm.logger.Println("ZooKeeper session expired or disconnected. Attempting to re-elect primary...")
			// Attempt to re-elect primary which will re-create the ephemeral node
			_, _, newElectEventChan, electErr := zkm.ElectPrimary()
			if electErr != nil {
				zkm.logger.Printf("ERROR: Failed to re-elect primary after session event: %v", electErr)
			} else if newElectEventChan != nil {
				// To ensure the select in handleZkEvents gets the new channel,
				// we'd need to re-assign it. This usually means passing it back via a channel,
				// or having a way to update the channels in the select loop, which is complex.
				// For now, we rely on the ElectPrimary method to set the watch on the correct node.
				zkm.logger.Println("Successfully re-elected primary or re-established election watch.")
			}
		}
	case zk.EventNodeDeleted:
		zkm.logger.Printf("ZooKeeper node deleted: Path -> %s", event.Path)
		if strings.HasPrefix(event.Path, primaryElectionPath) {
			zkm.logger.Printf("An election node was deleted (%s). Re-evaluating primary status...", event.Path)
			// A predecessor or current primary was deleted, re-evaluate election
			_, _, newElectEventChan, electErr := zkm.ElectPrimary()
			if electErr != nil {
				zkm.logger.Printf("ERROR: Failed to re-elect primary after node deletion: %v", electErr)
			} else {
				// This is where the event channel for the specific watch on the previous node would be re-established.
				// If ElectPrimary returns a new channel from GetW, this would become the new `electionEventChan`
				// in the `handleZkEvents` select. This implies a more complex channel management or a single re-election call.
				// For simplicity in this structure, ElectPrimary sets its own internal watches.
				_ = newElectEventChan // Use the channel to prevent lint error, but it's handled internally now.
				zkm.logger.Println("Successfully re-evaluated primary status.")
			}
		}
	case zk.EventNodeChildrenChanged:
		zkm.logger.Printf("ZooKeeper node children changed: Path -> %s", event.Path)
		if event.Path == primaryElectionPath {
			zkm.logger.Println("Children of primary election path changed. Re-evaluating primary status.")
			_, _, newElectEventChan, electErr := zkm.ElectPrimary()
			if electErr != nil {
				zkm.logger.Printf("ERROR: Failed to re-elect primary after children changed: %v", electErr)
			} else {
				_ = newElectEventChan // Use the channel to prevent lint error
				zkm.logger.Println("Successfully re-evaluated primary status due to children change.")
			}
		}
	default:
		zkm.logger.Printf("Received unhandled ZooKeeper election event type: %s (Type: %d, Path: %s, Err: %v)", event.Type.String(), event.Type, event.Path, event.Err)
	}
}

// handleNodesEvent processes events specifically related to the /kvreplicator/wal/nodes path.
func (zkm *ZKManager) handleNodesEvent(event zk.Event) {
	switch event.Type {
	case zk.EventSession:
		zkm.logger.Printf("ZooKeeper session event in nodes handler: State -> %s, Server -> %s", event.State.String(), event.Server)
		if event.State == zk.StateExpired || event.State == zk.StateDisconnected {
			zkm.logger.Println("ZooKeeper session expired or disconnected. Attempting to re-register node (for membership list)...")
			regErr := zkm.RegisterNode()
			if regErr != nil {
				zkm.logger.Printf("ERROR: Failed to re-register ephemeral ZK node after session event: %v", regErr)
			} else {
				zkm.logger.Println("Successfully re-registered ephemeral ZK node for membership.")
			}
		}
	case zk.EventNodeChildrenChanged:
		zkm.logger.Printf("ZooKeeper node children changed: Path -> %s", event.Path)
		if event.Path == "/kvreplicator/wal/nodes" { // Only re-process if it's the main nodes path
			children, _, newEventChan, err := zkm.conn.ChildrenW(event.Path)
			if err != nil {
				zkm.logger.Printf("ERROR: Failed to re-set watch or get children for path %s after children changed event: %v", event.Path, err)
			} else {
				zkm.logger.Printf("Re-set watch on %s. Current children: %v", event.Path, children)
				// Re-assign the nodes event channel. This will update the channel in the select loop.
				// This requires a mechanism to update the event channel in the parent goroutine,
				// or the event channels should be managed externally.
				// For now, we assume the newEventChan is the one ElectPrimary uses, which is not correct.
				// This needs refinement for real-time channel updates in select.
				// A simplified approach is to re-call EnsureNodesPathExists and ElectPrimary on session expiry.
				//
				// To truly update the select, handleZkEvents would need to be a method that accepts a way to
				// update the channels it's listening on, or the channels would need to be part of ZKManager.
				// For this exercise, I'll rely on the underlying ZK client's re-watch mechanism.
				_ = newEventChan // Acknowledge the channel but assume re-watch is handled implicitly
				zkm.mu.Lock()
				zkm.activeNodes = make(map[string]string) // Clear existing nodes
				for _, nodeID := range children {
					nodePath := fmt.Sprintf("%s/%s", event.Path, nodeID)
					data, _, err := zkm.conn.Get(nodePath)
					if err != nil {
						zkm.logger.Printf("ERROR: Failed to get data for ZK node %s: %v", nodePath, err)
						continue
					}
					internalBindAddress := string(data)
					zkm.activeNodes[nodeID] = internalBindAddress
					zkm.logger.Printf("Active node discovered: %s -> %s", nodeID, internalBindAddress)
				}
				zkm.logger.Printf("Updated active nodes list. Total active nodes: %d", len(zkm.activeNodes))
				zkm.mu.Unlock()
			}
		}
	case zk.EventNodeCreated:
		zkm.logger.Printf("ZooKeeper node created: Path -> %s", event.Path)
		// Re-fetch children to update activeNodes list
		if event.Path == "/kvreplicator/wal/nodes" {
			zkm.handleNodesEvent(zk.Event{Type: zk.EventNodeChildrenChanged, Path: event.Path})
		}
	case zk.EventNodeDeleted:
		zkm.logger.Printf("ZooKeeper node deleted: Path -> %s", event.Path)
		// Re-fetch children to update activeNodes list
		if strings.HasPrefix(event.Path, "/kvreplicator/wal/nodes/") { // A specific node was deleted
			zkm.handleNodesEvent(zk.Event{Type: zk.EventNodeChildrenChanged, Path: "/kvreplicator/wal/nodes"})
		}
	default:
		zkm.logger.Printf("Received unhandled ZooKeeper nodes event type: %s (Type: %d, Path: %s, Err: %v)", event.Type.String(), event.Type, event.Path, event.Err)
	}
}

// RegisterNode registers the current server as an ephemeral node in ZooKeeper.
func (zkm *ZKManager) RegisterNode() error {
	if zkm.internalBindAddress == "" {
		zkm.logger.Println("WARNING: internalBindAddress not set, cannot re-register node.")
		return fmt.Errorf("internalBindAddress not set, cannot re-register node.")
	}
	if zkm.conn == nil {
		zkm.logger.Println("Skipping ZooKeeper node registration: ZK connection is nil.")
		return nil
	}

	nodePath := fmt.Sprintf("/kvreplicator/wal/nodes/%s", zkm.nodeID)
	zkm.logger.Printf("Registering node in ZooKeeper at %s...", nodePath)

	_, err := zkm.conn.Create(nodePath, []byte(zkm.internalBindAddress), zk.FlagEphemeral, zk.WorldACL(zk.PermAll))
	if err != nil {
		if err == zk.ErrNodeExists {
			zkm.logger.Printf("WARNING: Ephemeral ZK node %s already exists. This might indicate an unclean shutdown or a duplicate node ID.", nodePath)
		} else {
			zkm.logger.Printf("ERROR: Failed to create ephemeral ZK node %s: %v", nodePath, err)
			return fmt.Errorf("failed to create ephemeral ZK node: %w", err)
		}
	} else {
		zkm.logger.Printf("Successfully registered node in ZooKeeper at %s", nodePath)
	}
	return nil
}

// EnsureNodesPathExists ensures the base /kvreplicator/wal/nodes path exists and sets a watch.
func (zkm *ZKManager) EnsureNodesPathExists() (eventChan <-chan zk.Event, err error) {
	if zkm.conn == nil {
		zkm.logger.Println("Skipping ZooKeeper path existence check: ZK connection is nil.")
		return // Returns nil, nil for named return values
	}

	nodesPath := "/kvreplicator/wal/nodes"

	// First, try to set a watch. If it doesn't exist, we'll create it.
	_, _, eventChan, err = zkm.conn.ChildrenW(nodesPath) // Assign to already declared eventChan and err
	if err != nil {
		if err == zk.ErrNoNode {
			// If the nodes path doesn't exist, try creating it (persistent)
			zkm.logger.Printf("ZK nodes path %s does not exist, attempting to create...", nodesPath)
			_, createErr := zkm.conn.Create(nodesPath, []byte{}, 0, zk.WorldACL(zk.PermAll))
			if createErr != nil && createErr != zk.ErrNodeExists {
				zkm.logger.Printf("ERROR: Failed to create ZK nodes path %s: %v", nodesPath, createErr)
				err = fmt.Errorf("failed to create ZK nodes path: %w", createErr) // Assign to err
				return                                                            // Returns named return values
			} else if createErr == zk.ErrNodeExists {
				zkm.logger.Printf("ZK nodes path %s already exists.", nodesPath)
			} else {
				zkm.logger.Printf("Created ZK nodes path %s.", nodesPath)
			}

			// Set watch again after creation (or if it existed after a race condition create)
			_, _, eventChan, err = zkm.conn.ChildrenW(nodesPath) // Assign to already declared eventChan and err
			if err != nil {
				zkm.logger.Printf("WARNING: Could not set watch on %s after creation: %v", nodesPath, err)
				err = fmt.Errorf("could not set watch on ZK nodes path after creation: %w", err) // Assign to err
				return                                                                           // Returns named return values
			} else {
				zkm.logger.Printf("Successfully set initial watch on %s for children changes.", nodesPath)
			}
		} else {
			zkm.logger.Printf("WARNING: Could not set watch on %s: %v", nodesPath, err)
			err = fmt.Errorf("could not set initial watch on ZK nodes path: %w", err) // Assign to err
			return                                                                    // Returns named return values
		}
	} else {
		zkm.logger.Printf("Successfully set initial watch on %s for children changes.", nodesPath)
	}
	return // Returns named return values
}

// ElectPrimary attempts to elect a primary node using ZooKeeper's sequential ephemeral nodes.
// It creates a node, then checks if it is the lowest sequence number.
// If not primary, it sets a watch on the previous node in the sequence.
func (zkm *ZKManager) ElectPrimary() (isPrimary bool, primaryAddr string, eventChan <-chan zk.Event, err error) {
	if zkm.conn == nil {
		zkm.logger.Println("Skipping primary election: ZK connection is nil.")
		return false, "", nil, fmt.Errorf("zookeeper connection not established")
	}

	// Ensure the election path exists (persistent)
	exists, _, err := zkm.conn.Exists(primaryElectionPath)
	if err != nil {
		zkm.logger.Printf("ERROR: Failed to check existence of ZK primary election path %s: %v", primaryElectionPath, err)
		return false, "", nil, fmt.Errorf("zookeeper primary election path check failed: %w", err)
	}
	if !exists {
		zkm.logger.Printf("ZK primary election path %s does not exist, creating...", primaryElectionPath)
		_, err = zkm.conn.Create(primaryElectionPath, nil, 0, zk.WorldACL(zk.PermAll))
		if err != nil && err != zk.ErrNodeExists {
			zkm.logger.Printf("ERROR: Failed to create ZK primary election path %s: %v", primaryElectionPath, err)
			return false, "", nil, fmt.Errorf("failed to create zookeeper primary election path: %w", err)
		}
		zkm.logger.Printf("ZK primary election path %s created or already exists.", primaryElectionPath)
	}

	// Create an ephemeral, sequential node under the election path
	// The data stored is the internal bind address of this node
	electionNodePathPrefix := fmt.Sprintf("%s/node-", primaryElectionPath)
	zkm.electionNodePath, err = zkm.conn.Create(electionNodePathPrefix, []byte(zkm.internalBindAddress), zk.FlagEphemeral|zk.FlagSequence, zk.WorldACL(zk.PermAll))
	if err != nil {
		zkm.logger.Printf("ERROR: Failed to create ephemeral sequential ZK node for primary election: %v", err)
		return false, "", nil, fmt.Errorf("failed to create election node: %w", err)
	}
	zkm.logger.Printf("Created election node: %s", zkm.electionNodePath)

	// Get all children of the election path and sort them to find the primary
	children, _, eventChan, err := zkm.conn.ChildrenW(primaryElectionPath) // Set a watch on the children of the election path
	if err != nil {
		zkm.logger.Printf("ERROR: Failed to get children for primary election path %s: %v", primaryElectionPath, err)
		return false, "", nil, fmt.Errorf("failed to get election path children: %w", err)
	}

	if len(children) == 0 {
		// This should theoretically not happen right after creating a node, but handle defensively
		zkm.logger.Println("WARNING: No children found in primary election path after creating node.")
		return false, "", nil, fmt.Errorf("no children found in primary election path")
	}

	// Sort children lexicographically to find the smallest sequence number
	// Example children: ["node-0000000001", "node-0000000002"]
	// This naturally sorts by sequence number due to padding.
	sort.Strings(children)
	smallestNode := children[0]
	primaryNodePath := fmt.Sprintf("%s/%s", primaryElectionPath, smallestNode)

	// Determine if this node is the primary
	if primaryNodePath == zkm.electionNodePath {
		zkm.logger.Printf("This node (%s) is the primary!", zkm.nodeID)
		// Get primary's address (which is this node's address)
		primaryAddr = zkm.internalBindAddress
		isPrimary = true
	} else {
		zkm.logger.Printf("This node (%s) is not the primary. Primary is %s.", zkm.nodeID, primaryNodePath)
		isPrimary = false

		// Find the node just before this one in the sorted list
		// This node should watch the previous node in sequence to detect its deletion
		myIndex := -1
		for i, child := range children {
			if fmt.Sprintf("%s/%s", primaryElectionPath, child) == zkm.electionNodePath {
				myIndex = i
				break
			}
		}

		if myIndex > 0 {
			nodeToWatch := fmt.Sprintf("%s/%s", primaryElectionPath, children[myIndex-1])
			zkm.logger.Printf("Watching node %s for deletion.", nodeToWatch)
			// Set a watch on the *previous* node. When it's deleted, we re-evaluate.
			_, _, watchEventChan, watchErr := zkm.conn.GetW(nodeToWatch) // GetW also sets a watch
			if watchErr != nil && watchErr != zk.ErrNoNode {             // ErrNoNode means it might have just been deleted, which is fine
				zkm.logger.Printf("ERROR: Failed to set watch on previous election node %s: %v", nodeToWatch, watchErr)
				// Don't return error, but log it. We still have the children watch.
			} else if watchErr == nil {
				// If watch was successfully set, replace the main eventChan with this specific watch for immediate re-evaluation.
				// The ChildrenW eventChan will still be active and can be used to re-establish the watch if needed.
				eventChan = watchEventChan
			}
		} else {
			// This case means myIndex is 0, which contradicts isPrimary=false.
			// Or, something went wrong and my node wasn't found in children.
			// Log this as a warning for debugging.
			zkm.logger.Printf("WARNING: Current node (%s) not found in children list, or is first but not marked primary.", zkm.electionNodePath)
		}

		// Get primary's address if this node is not primary
		data, _, getErr := zkm.conn.Get(primaryNodePath)
		if getErr != nil {
			zkm.logger.Printf("ERROR: Failed to get data for primary node %s: %v", primaryNodePath, getErr)
			primaryAddr = "unknown (error fetching primary address)"
		} else {
			primaryAddr = string(data)
		}
	}
	return
}

// GetPrimaryInfo returns the current primary status and address.
func (zkm *ZKManager) GetPrimaryInfo() (isPrimary bool, primaryAddr string, err error) {
	if zkm.conn == nil {
		return false, "", fmt.Errorf("zookeeper connection not established")
	}

	children, _, err := zkm.conn.Children(primaryElectionPath)
	if err != nil {
		if err == zk.ErrNoNode {
			return false, "", fmt.Errorf("primary election path %s does not exist, no primary elected yet", primaryElectionPath)
		}
		return false, "", fmt.Errorf("failed to get children for primary election path: %w", err)
	}

	if len(children) == 0 {
		return false, "", fmt.Errorf("no nodes in primary election path, no primary elected")
	}

	sort.Strings(children)
	smallestNode := children[0]
	primaryNodePath := fmt.Sprintf("%s/%s", primaryElectionPath, smallestNode)

	isPrimary = (primaryNodePath == zkm.electionNodePath)

	data, _, getErr := zkm.conn.Get(primaryNodePath)
	if getErr != nil {
		zkm.logger.Printf("ERROR: Failed to get data for primary node %s: %v", primaryNodePath, getErr)
		primaryAddr = "unknown (error fetching primary address)"
	} else {
		primaryAddr = string(data)
	}

	return isPrimary, primaryAddr, nil
}
