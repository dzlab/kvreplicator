package wal_replicator

import (
	"fmt"
	"log"
	"os"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/go-zookeeper/zk"
)

// mockNodeChangeCallback is a dummy callback function for testing purposes.
func mockNodeChangeCallback(nodes map[string]string) {
	// This function does nothing, but it fulfills the NodeChangeCallback type.
	// In a more complex test, one might use a channel or a mock object to verify
	// that this callback was indeed invoked with expected arguments.
}

// TestNewZKManager verifies that NewZKManager correctly initializes a ZKManager instance.
func TestNewZKManager(t *testing.T) {
	logger := log.New(os.Stdout, "TEST: ", log.LstdFlags)
	nodeID := "test-node-1"

	// Test with a valid callback
	zkm := NewZKManager(logger, nodeID, mockNodeChangeCallback)

	if zkm == nil {
		t.Fatal("NewZKManager returned nil, expected a ZKManager instance")
	}
	if zkm.logger != logger {
		t.Errorf("NewZKManager did not set the logger correctly. Expected %p, got %p", logger, zkm.logger)
	}
	if zkm.nodeID != nodeID {
		t.Errorf("NewZKManager did not set the nodeID correctly. Expected %q, got %q", nodeID, zkm.nodeID)
	}
	if zkm.activeNodes == nil {
		t.Error("NewZKManager did not initialize activeNodes map. Expected a map, got nil")
	}
	if len(zkm.activeNodes) != 0 {
		t.Errorf("NewZKManager initialized activeNodes with content. Expected empty map, got %v", zkm.activeNodes)
	}
	// Use reflect.ValueOf().Pointer() to compare function pointers, ensuring the callback was set.
	if reflect.ValueOf(zkm.nodeChangeCallback).Pointer() != reflect.ValueOf(mockNodeChangeCallback).Pointer() {
		t.Error("NewZKManager did not set the nodeChangeCallback correctly. Expected mockNodeChangeCallback")
	}

	// Test with a nil callback
	zkmNoCallback := NewZKManager(logger, "test-node-2", nil)
	if zkmNoCallback.nodeChangeCallback != nil {
		t.Error("NewZKManager did not set nodeChangeCallback to nil when provided nil.")
	}
}

// TestGetActiveNodes tests the GetActiveNodes method of ZKManager.
// It verifies that the method returns a correct copy of the internal activeNodes map.
func TestGetActiveNodes(t *testing.T) {
	// Initialize a ZKManager instance for testing.
	// We only need the activeNodes map and its mutex for this specific test.
	zkm := &ZKManager{
		logger:      log.New(os.Stdout, "TEST: ", log.LstdFlags),
		nodeID:      "test-node",
		activeNodes: make(map[string]string),
		mu:          sync.RWMutex{},
	}

	// Case 1: Internal activeNodes map is empty.
	t.Run("EmptyActiveNodes", func(t *testing.T) {
		zkm.mu.Lock()
		zkm.activeNodes = make(map[string]string) // Ensure it's empty
		zkm.mu.Unlock()

		nodes := zkm.GetActiveNodes()

		if len(nodes) != 0 {
			t.Errorf("Expected 0 active nodes, got %d", len(nodes))
		}
		// Verify that a copy is returned by checking its independence (further verified in CopyIndependence sub-test).
		// Direct map comparison `==` is not possible in Go.
	})

	// Case 2: Internal activeNodes map contains data.
	t.Run("PopulatedActiveNodes", func(t *testing.T) {
		expectedNodes := map[string]string{
			"nodeA": "192.168.1.1:8080",
			"nodeB": "192.168.1.2:8080",
		}

		// Manually populate the internal activeNodes map
		zkm.mu.Lock()
		zkm.activeNodes = make(map[string]string) // Clear previous state
		for k, v := range expectedNodes {
			zkm.activeNodes[k] = v
		}
		zkm.mu.Unlock()

		nodes := zkm.GetActiveNodes()

		// Verify that the returned map has the same length and content
		if len(nodes) != len(expectedNodes) {
			t.Errorf("Expected %d active nodes, got %d", len(expectedNodes), len(nodes))
		}

		for k, v := range expectedNodes {
			if val, ok := nodes[k]; !ok || val != v {
				t.Errorf("Expected node %q with address %q, got %q (found: %t)", k, v, val, ok)
			}
		}

		// Verify that a copy is returned by checking its independence (further verified in this sub-test).
		// Direct map comparison `==` is not possible in Go.
		// Case 3: Verify copy independence (modifying the returned map does not affect the original).
		// This is critical for ensuring the returned map is a true copy.
		t.Run("CopyIndependence", func(t *testing.T) {
			nodesCopy := zkm.GetActiveNodes() // Get a fresh copy

			// Modify the returned map
			nodesCopy["nodeC"] = "192.168.1.3:8080"
			delete(nodesCopy, "nodeA")

			// Check if the internal map remains unchanged
			zkm.mu.RLock()
			defer zkm.mu.RUnlock()

			if len(zkm.activeNodes) != len(expectedNodes) {
				t.Errorf("Internal map length changed unexpectedly. Expected %d, got %d", len(expectedNodes), len(zkm.activeNodes))
			}
			if _, exists := zkm.activeNodes["nodeC"]; exists {
				t.Error("Modifying the returned map unexpectedly added 'nodeC' to the internal map")
			}
			if _, exists := zkm.activeNodes["nodeA"]; !exists {
				t.Error("Modifying the returned map unexpectedly removed 'nodeA' from the internal map")
			}
			// Specifically check the original values are still there
			if zkm.activeNodes["nodeA"] != "192.168.1.1:8080" {
				t.Errorf("Original value for nodeA changed unexpectedly: %s", zkm.activeNodes["nodeA"])
			}
		})
	})
}

// mockZKConnection is a mock implementation of the ZKConnection interface for testing.
type mockZKConnection struct {
	childrenWFunc func(path string) ([]string, *zk.Stat, <-chan zk.Event, error)
	getFunc       func(path string) ([]byte, *zk.Stat, error)
	existsFunc    func(path string) (bool, *zk.Stat, error)
	createFunc    func(path string, data []byte, flags int32, acl []zk.ACL) (string, error)
	deleteFunc    func(path string, version int32) error
	childrenFunc  func(path string) ([]string, *zk.Stat, error)
	existsWFunc   func(path string) (bool, *zk.Stat, <-chan zk.Event, error)
	closeFunc     func()
}

// Ensure mockZKConnection implements ZKConnection
var _ ZKConnection = (*mockZKConnection)(nil)

func (m *mockZKConnection) ChildrenW(path string) ([]string, *zk.Stat, <-chan zk.Event, error) {
	if m.childrenWFunc != nil {
		return m.childrenWFunc(path)
	}
	return nil, nil, nil, fmt.Errorf("ChildrenW not implemented")
}
func (m *mockZKConnection) Get(path string) ([]byte, *zk.Stat, error) {
	if m.getFunc != nil {
		return m.getFunc(path)
	}
	return nil, nil, fmt.Errorf("Get not implemented")
}
func (m *mockZKConnection) Exists(path string) (bool, *zk.Stat, error) {
	if m.existsFunc != nil {
		return m.existsFunc(path)
	}
	return false, nil, fmt.Errorf("Exists not implemented")
}
func (m *mockZKConnection) Create(path string, data []byte, flags int32, acl []zk.ACL) (string, error) {
	if m.createFunc != nil {
		return m.createFunc(path, data, flags, acl)
	}
	return "", fmt.Errorf("Create not implemented")
}
func (m *mockZKConnection) Delete(path string, version int32) error {
	if m.deleteFunc != nil {
		return m.deleteFunc(path, version)
	}
	return fmt.Errorf("Delete not implemented")
}
func (m *mockZKConnection) Children(path string) ([]string, *zk.Stat, error) {
	if m.childrenFunc != nil {
		return m.childrenFunc(path)
	}
	return nil, nil, fmt.Errorf("Children not implemented")
}
func (m *mockZKConnection) ExistsW(path string) (bool, *zk.Stat, <-chan zk.Event, error) {
	if m.existsWFunc != nil {
		return m.existsWFunc(path)
	}
	return false, nil, nil, fmt.Errorf("ExistsW not implemented")
}
func (m *mockZKConnection) Close() {
	if m.closeFunc != nil {
		m.closeFunc()
	}
}

// TestHandleZkEvents_NodeChildrenChanged tests the ZKManager's ability to react
// to zk.EventNodeChildrenChanged events by attempting to refresh active nodes
// and invoking the nodeChangeCallback.
func TestHandleZkEvents_NodeChildrenChanged(t *testing.T) {
	logger := log.New(os.Stdout, "TEST_EVENTS: ", log.LstdFlags)
	nodeID := "test-event-node"

	var receivedNodes map[string]string
	callbackCalled := make(chan struct{})

	mockCallback := func(nodes map[string]string) {
		receivedNodes = nodes
		close(callbackCalled)
	}

	zkm := NewZKManager(logger, nodeID, mockCallback)
	zkm.internalBindAddress = "127.0.0.1:9090"

	// Mock the ZKConnection for this test
	mockConn := &mockZKConnection{}

	// Channels for watch events
	nodeEventWatchChan := make(chan zk.Event, 1) // Buffered to prevent deadlock on send
	// We only expect ChildrenW to be called once by watchActiveNodes initially
	// or on re-watch. Simulate a children list and the event channel for the next watch.
	mockConn.childrenWFunc = func(path string) ([]string, *zk.Stat, <-chan zk.Event, error) {
		if path == nodesPath {
			// Return some mock children and the event channel for future watches
			mockChildren := []string{"node1", "node2"}
			return mockChildren, &zk.Stat{}, nodeEventWatchChan, nil
		}
		return nil, nil, nil, fmt.Errorf("unexpected ChildrenW path: %s", path)
	}

	// Simulate Get calls for the mock children
	mockConn.getFunc = func(path string) ([]byte, *zk.Stat, error) {
		switch path {
		case fmt.Sprintf("%s/%s", nodesPath, "node1"):
			return []byte("192.168.0.1:8080"), &zk.Stat{}, nil
		case fmt.Sprintf("%s/%s", nodesPath, "node2"):
			return []byte("192.168.0.2:8080"), &zk.Stat{}, nil
		default:
			return nil, nil, fmt.Errorf("unexpected Get path: %s", path)
		}
	}

	// Assign the mock connection to the ZKManager
	zkm.conn = mockConn

	// Create channels that handleZkEvents will listen to.
	// These would normally be returned by ElectPrimary and watchActiveNodes.
	mockElectionEvents := make(chan zk.Event)
	// The `nodeEventWatchChan` returned by `mockConn.childrenWFunc` is what `handleZkEvents` will listen to.

	// Start the event handler goroutine.
	go zkm.handleZkEvents(mockElectionEvents, nodeEventWatchChan)

	// Simulate an initial successful watch and update.
	// This is implicitly handled by the first call to watchActiveNodes from Start().
	// For this test, we are directly sending the event AFTER `handleZkEvents` starts.
	// The `watchActiveNodes` will be triggered by the `handleNodesEvent` function.

	// Give time for handleZkEvents to start and potentially make its initial watch.
	time.Sleep(100 * time.Millisecond)

	// Simulate a zk.EventNodeChildrenChanged event for the nodes path.
	simulatedEvent := zk.Event{
		Type: zk.EventNodeChildrenChanged,
		Path: nodesPath, // Corresponds to the path watched by watchActiveNodes
	}

	// Send the simulated event to the nodeEvents channel.
	nodeEventWatchChan <- simulatedEvent

	// Wait for the callback to be invoked or timeout.
	select {
	case <-callbackCalled:
		t.Log("NodeChangeCallback was invoked.")
		expectedNodes := map[string]string{
			"node1": "192.168.0.1:8080",
			"node2": "192.168.0.2:8080",
		}
		if !reflect.DeepEqual(receivedNodes, expectedNodes) {
			t.Errorf("Received nodes mismatch. Expected %v, got %v", expectedNodes, receivedNodes)
		}
	case <-time.After(time.Second * 3):
		t.Fatal("Timed out waiting for NodeChangeCallback to be invoked after EventNodeChildrenChanged.")
	}

	// Close channels to signal the handleZkEvents goroutine to exit cleanly.
	close(nodeEventWatchChan)
	close(mockElectionEvents)

	// Give the goroutine a moment to finish
	time.Sleep(100 * time.Millisecond)
}
