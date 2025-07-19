package wal_replicator

import (
	"log"
	"os"
	"reflect"
	"sync"
	"testing"
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
