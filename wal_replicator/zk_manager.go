package wal_replicator

import (
	"fmt"
	"log"

	"github.com/go-zookeeper/zk"
)

// ZKManager handles ZooKeeper operations for WALReplicationServer.
type ZKManager struct {
	conn   *zk.Conn
	logger *log.Logger
	nodeID string
}

// NewZKManager creates a new ZKManager instance.
func NewZKManager(conn *zk.Conn, logger *log.Logger, nodeID string) *ZKManager {
	return &ZKManager{
		conn:   conn,
		logger: logger,
		nodeID: nodeID,
	}
}

// RegisterNode registers the current server as an ephemeral node in ZooKeeper.
func (zkm *ZKManager) RegisterNode(internalBindAddress string) error {
	if zkm.conn == nil {
		zkm.logger.Println("Skipping ZooKeeper node registration: ZK connection is nil.")
		return nil
	}

	nodePath := fmt.Sprintf("/kvreplicator/wal/nodes/%s", zkm.nodeID)
	zkm.logger.Printf("Registering node in ZooKeeper at %s...", nodePath)

	_, err := zkm.conn.Create(nodePath, []byte(internalBindAddress), zk.FlagEphemeral, zk.WorldACL(zk.PermAll))
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
