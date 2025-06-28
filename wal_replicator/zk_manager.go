package wal_replicator

import (
	"fmt"
	"log"
	"sync" // Add sync package for mutex
	"time" // Add time package for timeouts

	"github.com/go-zookeeper/zk"
	// Add context package for shutdown
)

// ZKManager handles ZooKeeper operations for WALReplicationServer.
type ZKManager struct {
	conn                *zk.Conn
	logger              *log.Logger
	nodeID              string
	internalBindAddress string            // Store internal bind address for re-registration
	activeNodes         map[string]string // Map of nodeID to internalBindAddress
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

	eventChan, err := zkm.EnsureNodesPathExists()
	if err != nil {
		zkm.logger.Printf("Error ensuring ZK nodes path exists or setting watch: %v", err)
		return err // Or decide if it's a fatal error
	} else if eventChan != nil {
		go zkm.handleZkEvents(eventChan) // Start goroutine to handle ZooKeeper events
	}
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
func (zkm *ZKManager) handleZkEvents(initialEventChan <-chan zk.Event) {
	zkm.logger.Println("Starting ZooKeeper event handler goroutine.")

	currentEventChan := initialEventChan

	for {
		select {
		case event, ok := <-currentEventChan:
			if !ok {
				zkm.logger.Println("ZooKeeper event channel closed. Stopping event handler.")
				return // Exit the goroutine if the channel is closed
			}
			zkm.logger.Printf("Received ZK event: %+v", event)
			switch event.Type {
			case zk.EventSession:
				zkm.logger.Printf("ZooKeeper session event: State -> %s, Server -> %s", event.State.String(), event.Server)
				if event.State == zk.StateExpired || event.State == zk.StateDisconnected {
					zkm.logger.Println("ZooKeeper session expired or disconnected. Attempting to re-register node...")
					// Attempt to re-register the ephemeral node
					regErr := zkm.RegisterNode()
					if regErr != nil {
						zkm.logger.Printf("ERROR: Failed to re-register ephemeral ZK node after session event: %v", regErr)
					} else {
						zkm.logger.Println("Successfully re-registered ephemeral ZK node.")
					}
				}
			case zk.EventNodeCreated:
				zkm.logger.Printf("ZooKeeper node created: Path -> %s", event.Path)
				// TODO: Implement logic for node creation events (e.g., watch data for new nodes)
			case zk.EventNodeDeleted:
				zkm.logger.Printf("ZooKeeper node deleted: Path -> %s", event.Path)
				// TODO: Implement logic for node deletion events (e.g., remove from list of active nodes)
			case zk.EventNodeDataChanged:
				zkm.logger.Printf("ZooKeeper node data changed: Path -> %s", event.Path)
				// TODO: Implement logic for node data changes (e.g., primary node data)
			case zk.EventNodeChildrenChanged:
				zkm.logger.Printf("ZooKeeper node children changed: Path -> %s", event.Path)
				children, _, newEventChan, err := zkm.conn.ChildrenW(event.Path)
				if err != nil {
					zkm.logger.Printf("ERROR: Failed to re-set watch or get children for path %s after children changed event: %v", event.Path, err)
				} else {
					zkm.logger.Printf("Re-set watch on %s. Current children: %v", event.Path, children)
					currentEventChan = newEventChan
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
			default:
				zkm.logger.Printf("Received unhandled ZooKeeper event type: %s (Type: %d, Path: %s, Err: %v)", event.Type.String(), event.Type, event.Path, event.Err)
			}
		}
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
