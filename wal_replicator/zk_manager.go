package wal_replicator

import (
	"fmt"
	"log"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/go-zookeeper/zk"
)

const primaryElectionPath = "/kvreplicator/wal/primary_election"
const nodesPath = "/kvreplicator/wal/nodes" // Define nodesPath as a constant

// ZKManager handles ZooKeeper operations for WALReplicationServer.
type ZKManager struct {
	conn                *zk.Conn
	logger              *log.Logger
	nodeID              string
	internalBindAddress string            // Store internal bind address for re-registration
	activeNodes         map[string]string // Map of nodeID to internalBindAddress, managed by ZK events
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

	// Ensure base paths exist
	paths := []string{"/kvreplicator", "/kvreplicator/wal", nodesPath, primaryElectionPath}
	for _, path := range paths {
		exists, _, err := zkm.conn.Exists(path)
		if err != nil {
			zkm.conn.Close()
			zkm.conn = nil
			return fmt.Errorf("failed to check existence of ZK path %s: %w", path, err)
		}
		if !exists {
			zkm.logger.Printf("ZK path %s does not exist, creating...", path)
			_, err = zkm.conn.Create(path, nil, 0, zk.WorldACL(zk.PermAll))
			if err != nil && err != zk.ErrNodeExists {
				zkm.conn.Close()
				zkm.conn = nil
				return fmt.Errorf("failed to create ZK path %s: %w", path, err)
			}
		}
	}
	zkm.logger.Println("ZooKeeper base paths ensured.")
	return nil
}

// Start registers the node, starts primary election, and sets up event handling.
func (zkm *ZKManager) Start(internalBindAddress string) error {
	if zkm.conn == nil {
		zkm.logger.Println("Skipping ZKManager Start: ZK connection is nil.")
		return nil
	}

	zkm.internalBindAddress = internalBindAddress

	// Register this node for membership.
	if err := zkm.RegisterNode(); err != nil {
		return err // This is a critical failure.
	}

	// Watch for changes in the list of active nodes (membership).
	nodesEventChan, err := zkm.watchActiveNodes()
	if err != nil {
		return err // Also critical.
	}

	// Participate in the primary election.
	_, _, electionEventChan, err := zkm.ElectPrimary()
	if err != nil {
		return err // Also critical.
	}

	// Start a single goroutine to handle all ZooKeeper events.
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

// GetActiveNodes returns a copy of the current map of active nodes.
func (zkm *ZKManager) GetActiveNodes() map[string]string {
	zkm.mu.RLock()
	defer zkm.mu.RUnlock()
	copiedNodes := make(map[string]string, len(zkm.activeNodes))
	for k, v := range zkm.activeNodes {
		copiedNodes[k] = v
	}
	return copiedNodes
}

// handleZkEvents processes events received from ZooKeeper in a loop.
func (zkm *ZKManager) handleZkEvents(electionEvents <-chan zk.Event, nodeEvents <-chan zk.Event) {
	zkm.logger.Println("Starting ZooKeeper event handler goroutine.")
	for {
		select {
		case event, ok := <-electionEvents:
			if !ok {
				zkm.logger.Println("ZooKeeper election event channel closed. Exiting event handler for elections.")
				electionEvents = nil // Prevent this case from firing again.
			} else {
				zkm.logger.Printf("Received ZK election event: %+v", event)
				newElectionEvents := zkm.handleElectionEvent(event)
				if newElectionEvents != nil {
					electionEvents = newElectionEvents // Update to the new channel
				}
			}
		case event, ok := <-nodeEvents:
			if !ok {
				zkm.logger.Println("ZooKeeper nodes event channel closed. Exiting event handler for nodes.")
				nodeEvents = nil // Prevent this case from firing again.
			} else {
				zkm.logger.Printf("Received ZK nodes event: %+v", event)
				newNodeEvents := zkm.handleNodesEvent(event)
				if newNodeEvents != nil {
					nodeEvents = newNodeEvents // Update to the new channel
				}
			}
		}
		if electionEvents == nil && nodeEvents == nil {
			zkm.logger.Println("All ZK event channels are closed. Stopping event handler goroutine.")
			return
		}
	}
}

// handleElectionEvent processes primary election events and returns a new event channel if a new watch is set.
func (zkm *ZKManager) handleElectionEvent(event zk.Event) <-chan zk.Event {
	// Re-run the election process on any relevant event. ElectPrimary is idempotent.
	if event.Type == zk.EventNodeDeleted || event.Type == zk.EventNodeChildrenChanged || event.State == zk.StateHasSession {
		zkm.logger.Printf("Re-evaluating primary status due to event: %s", event.Type)
		_, _, newEventChan, err := zkm.ElectPrimary()
		if err != nil {
			zkm.logger.Printf("ERROR: Failed to re-elect primary after event: %v", err)
			return nil
		}
		return newEventChan
	}
	if event.State == zk.StateExpired {
		zkm.logger.Println("Session expired. Full re-registration and re-election required.")
		// The connection is managed externally; here we just signal that re-election is needed.
		// The Start() method should be called again upon reconnection.
	}
	return nil
}

// handleNodesEvent processes membership changes and returns a new event channel.
func (zkm *ZKManager) handleNodesEvent(event zk.Event) <-chan zk.Event {
	// If children change, re-watch and update the active nodes list.
	if event.Type == zk.EventNodeChildrenChanged || event.Type == zk.EventNodeCreated || event.Type == zk.EventNodeDeleted {
		zkm.logger.Printf("Membership changed due to event: %s on path %s. Refreshing active nodes list.", event.Type, event.Path)
		newEventChan, err := zkm.watchActiveNodes()
		if err != nil {
			zkm.logger.Printf("ERROR: Failed to re-watch active nodes after change event: %v", err)
			return nil
		}
		return newEventChan
	}
	if event.State == zk.StateExpired {
		zkm.logger.Println("Session expired. Re-registering node for membership.")
		if err := zkm.RegisterNode(); err != nil {
			zkm.logger.Printf("ERROR: Failed to re-register node after session expiry: %v", err)
		}
	}
	return nil
}

// watchActiveNodes gets the list of active nodes, updates the local cache, and sets a watch.
func (zkm *ZKManager) watchActiveNodes() (<-chan zk.Event, error) {
	children, _, eventChan, err := zkm.conn.ChildrenW(nodesPath)
	if err != nil {
		zkm.logger.Printf("ERROR: Failed to get or watch children for path %s: %v", nodesPath, err)
		return nil, fmt.Errorf("failed to watch active nodes: %w", err)
	}

	zkm.logger.Printf("Updating active nodes. Found %d nodes: %v", len(children), children)

	newActiveNodes := make(map[string]string)
	for _, nodeID := range children {
		nodePath := fmt.Sprintf("%s/%s", nodesPath, nodeID)
		data, _, err := zkm.conn.Get(nodePath)
		if err != nil {
			// This can happen if a node disappears between ChildrenW and Get. It's usually safe to ignore.
			zkm.logger.Printf("WARNING: Failed to get data for node %s, it may have been removed: %v", nodePath, err)
			continue
		}
		newActiveNodes[nodeID] = string(data)
	}

	zkm.mu.Lock()
	zkm.activeNodes = newActiveNodes
	zkm.mu.Unlock()

	zkm.logger.Printf("Successfully updated active nodes list: %v", zkm.GetActiveNodes())
	return eventChan, nil
}

// RegisterNode registers the current server as an ephemeral node in ZooKeeper for membership.
func (zkm *ZKManager) RegisterNode() error {
	if zkm.internalBindAddress == "" {
		return fmt.Errorf("internalBindAddress not set, cannot register node")
	}
	if zkm.conn == nil {
		return fmt.Errorf("cannot register node, ZK connection is nil")
	}

	nodePath := fmt.Sprintf("%s/%s", nodesPath, zkm.nodeID)
	zkm.logger.Printf("Registering node for membership in ZooKeeper at %s...", nodePath)

	// Check if the node already exists. If so, delete it before recreating.
	exists, _, err := zkm.conn.Exists(nodePath)
	if err != nil {
		return fmt.Errorf("failed to check for existing node %s: %w", nodePath, err)
	}
	if exists {
		zkm.logger.Printf("WARNING: Node %s already exists. Deleting it before re-registering. This could indicate a previous unclean shutdown.", nodePath)
		if err := zkm.conn.Delete(nodePath, -1); err != nil {
			// It might be an ephemeral node from another session; non-fatal error.
			zkm.logger.Printf("WARNING: Failed to delete existing node %s: %v", nodePath, err)
		}
	}

	_, err = zkm.conn.Create(nodePath, []byte(zkm.internalBindAddress), zk.FlagEphemeral, zk.WorldACL(zk.PermAll))
	if err != nil {
		if err == zk.ErrNodeExists {
			zkm.logger.Printf("Node %s already exists, assuming it's ours from a race condition.", nodePath)
			return nil
		}
		return fmt.Errorf("failed to create ephemeral node %s: %w", nodePath, err)
	}

	zkm.logger.Printf("Successfully registered node in ZooKeeper at %s", nodePath)
	return nil
}

// ElectPrimary attempts to elect a primary node using ZooKeeper's sequential ephemeral nodes.
func (zkm *ZKManager) ElectPrimary() (isPrimary bool, primaryAddr string, eventChan <-chan zk.Event, err error) {
	if zkm.conn == nil {
		return false, "", nil, fmt.Errorf("zookeeper connection not established")
	}

	// Create an ephemeral, sequential node for the election
	electionNodePathPrefix := fmt.Sprintf("%s/node-", primaryElectionPath)
	// If we already have an election node, check if it's still valid.
	if zkm.electionNodePath != "" {
		exists, _, err := zkm.conn.Exists(zkm.electionNodePath)
		if err != nil {
			return false, "", nil, fmt.Errorf("failed to check existing election node: %w", err)
		}
		if !exists {
			zkm.electionNodePath = "" // Our old node is gone, we need a new one.
		}
	}

	if zkm.electionNodePath == "" {
		zkm.electionNodePath, err = zkm.conn.Create(electionNodePathPrefix, []byte(zkm.internalBindAddress), zk.FlagEphemeral|zk.FlagSequence, zk.WorldACL(zk.PermAll))
		if err != nil {
			return false, "", nil, fmt.Errorf("failed to create election node: %w", err)
		}
		zkm.logger.Printf("Created election node: %s", zkm.electionNodePath)
	}

	// Get all children and find the primary
	children, _, err := zkm.conn.Children(primaryElectionPath)
	if err != nil {
		return false, "", nil, fmt.Errorf("failed to get election path children: %w", err)
	}

	if len(children) == 0 {
		// This can happen in a race condition, try again.
		time.Sleep(100 * time.Millisecond)
		return zkm.ElectPrimary()
	}

	sort.Strings(children)
	primaryNodeZNode := children[0]
	primaryNodePath := fmt.Sprintf("%s/%s", primaryElectionPath, primaryNodeZNode)

	// Determine if this node is the primary
	if primaryNodePath == zkm.electionNodePath {
		zkm.logger.Printf("This node (%s) is the primary.", zkm.nodeID)
		isPrimary = true
		primaryAddr = zkm.internalBindAddress
		// As primary, watch for any changes in children to detect new nodes joining.
		_, _, eventChan, err = zkm.conn.ChildrenW(primaryElectionPath)
		if err != nil {
			return false, "", nil, fmt.Errorf("primary failed to set ChildrenW watch: %w", err)
		}
	} else {
		zkm.logger.Printf("This node (%s) is not primary. Primary is %s.", zkm.nodeID, primaryNodePath)
		isPrimary = false

		// Get primary's address
		data, _, getErr := zkm.conn.Get(primaryNodePath)
		if getErr != nil {
			primaryAddr = "unknown (error fetching primary address)"
		} else {
			primaryAddr = string(data)
		}

		// Watch the node just before this one in the sequence.
		myIndex := sort.SearchStrings(children, strings.TrimPrefix(zkm.electionNodePath, primaryElectionPath+"/"))
		if myIndex > 0 {
			nodeToWatchPath := fmt.Sprintf("%s/%s", primaryElectionPath, children[myIndex-1])
			zkm.logger.Printf("Watching predecessor node %s for deletion.", nodeToWatchPath)
			exists, _, watchChan, watchErr := zkm.conn.ExistsW(nodeToWatchPath)
			if watchErr != nil {
				return false, "", nil, fmt.Errorf("failed to set ExistsW watch on %s: %w", nodeToWatchPath, watchErr)
			}
			if !exists {
				// The node we wanted to watch is already gone, so re-run election immediately.
				return zkm.ElectPrimary()
			}
			eventChan = watchChan // This is the channel we care about.
		} else {
			// This shouldn't happen if we're not the primary. Fallback to watching children.
			zkm.logger.Printf("WARNING: Could not find predecessor to watch for node %s, falling back to watching all children.", zkm.electionNodePath)
			_, _, eventChan, err = zkm.conn.ChildrenW(primaryElectionPath)
			if err != nil {
				return false, "", nil, fmt.Errorf("fallback ChildrenW watch failed: %w", err)
			}
		}
	}
	return
}

// GetPrimaryInfo returns the current primary status and address without setting new watches.
func (zkm *ZKManager) GetPrimaryInfo() (isPrimary bool, primaryAddr string, err error) {
	if zkm.conn == nil {
		return false, "", fmt.Errorf("zookeeper connection not established")
	}

	children, _, err := zkm.conn.Children(primaryElectionPath)
	if err != nil {
		if err == zk.ErrNoNode {
			return false, "", fmt.Errorf("primary election path %s does not exist", primaryElectionPath)
		}
		return false, "", fmt.Errorf("failed to get children for primary election path: %w", err)
	}

	if len(children) == 0 {
		return false, "", nil // No primary elected yet.
	}

	sort.Strings(children)
	primaryNodeZNode := children[0]
	primaryNodePath := fmt.Sprintf("%s/%s", primaryElectionPath, primaryNodeZNode)

	// electionNodePath might be empty if ElectPrimary hasn't run successfully yet
	if zkm.electionNodePath != "" {
		isPrimary = (primaryNodePath == zkm.electionNodePath)
	}

	data, _, getErr := zkm.conn.Get(primaryNodePath)
	if getErr != nil {
		primaryAddr = "unknown (error fetching primary address)"
	} else {
		primaryAddr = string(data)
	}

	return isPrimary, primaryAddr, nil
}
