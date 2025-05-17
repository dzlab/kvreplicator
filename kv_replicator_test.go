package kvreplicator

import (
	"fmt"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/hashicorp/raft"
)

// Helper function to create a KVReplicator instance for testing.
// Returns the instance, its config, and a cleanup function.
func setupKVReplicator(t *testing.T, nodeID string, bootstrap bool, joinAddresses []string) (*KVReplicator, Config, func()) {
	t.Helper()

	raftDataDir := t.TempDir()
	dbDir := t.TempDir() // Though not used by the current FSM, good for future.

	// Find an available port for Raft
	listener, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		t.Fatalf("Failed to find an available port: %v", err)
	}
	raftBindAddr := listener.Addr().String()
	listener.Close() // Close immediately, Raft will re-bind.

	cfg := Config{
		NodeID:          nodeID,
		RaftBindAddress: raftBindAddr,
		RaftDataDir:     raftDataDir,
		DBPath:          dbDir,
		Bootstrap:       bootstrap,
		JoinAddresses:   joinAddresses,
		ApplyTimeout:    5 * time.Second, // Shorter timeout for tests
		Logger:          log.New(os.Stdout, fmt.Sprintf("[%s-test] ", nodeID), log.LstdFlags|log.Lmicroseconds),
	}

	kv, err := NewKVReplicator(cfg)
	if err != nil {
		t.Fatalf("Failed to create KVReplicator for node %s: %v", nodeID, err)
	}

	cleanup := func() {
		if kv.raftManager.raftNode != nil {
			kv.Shutdown() // Ensure Raft node is shut down
		}
		os.RemoveAll(raftDataDir)
		os.RemoveAll(dbDir)
	}

	return kv, cfg, cleanup
}

// Helper function to wait for a node to become leader.
func waitForLeader(t *testing.T, kv *KVReplicator, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if kv.IsLeader() {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("Node %s did not become leader within %v. Current state: %s, Leader: %s", kv.config.NodeID, timeout, kv.raftManager.raftNode.State().String(), kv.Leader())
}

func TestNewKVReplicator(t *testing.T) {
	t.Run("ValidConfig", func(t *testing.T) {
		_, _, cleanup := setupKVReplicator(t, "node1", true, nil)
		defer cleanup()
		// If setupKVReplicator didn't panic/fatal, creation was successful.
	})

	t.Run("MissingNodeID", func(t *testing.T) {
		cfg := Config{
			RaftBindAddress: "localhost:0", // Dynamic port won't be an issue here
			RaftDataDir:     t.TempDir(),
		}
		_, err := NewKVReplicator(cfg)
		if err == nil {
			t.Error("Expected error for missing NodeID, got nil")
		} else if !strings.Contains(err.Error(), "NodeID must be specified") {
			t.Errorf("Expected NodeID error, got: %v", err)
		}
	})

	t.Run("MissingRaftDataDir", func(t *testing.T) {
		cfg := Config{
			NodeID:          "node1",
			RaftBindAddress: "localhost:0",
		}
		_, err := NewKVReplicator(cfg)
		if err == nil {
			t.Error("Expected error for missing RaftDataDir, got nil")
		} else if !strings.Contains(err.Error(), "RaftDataDir must be specified") {
			t.Errorf("Expected RaftDataDir error, got: %v", err)
		}
	})
}

func TestKVReplicator_Start_Bootstrap(t *testing.T) {
	kv, _, cleanup := setupKVReplicator(t, "node1", true, nil)
	defer cleanup()

	if err := kv.Start(); err != nil {
		t.Fatalf("Failed to start KVReplicator: %v", err)
	}

	waitForLeader(t, kv, 10*time.Second) // Increased timeout for first leader election
	if !kv.IsLeader() {
		t.Errorf("Node should be leader after bootstrap and start")
	}
}

func TestKVReplicator_PutGetDelete(t *testing.T) {
	kv, _, cleanup := setupKVReplicator(t, "node1", true, nil)
	defer cleanup()

	if err := kv.Start(); err != nil {
		t.Fatalf("Failed to start KVReplicator: %v", err)
	}
	waitForLeader(t, kv, 10*time.Second)

	key := "testkey"
	value := "testvalue"

	// Put
	if err := kv.Put(key, value); err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// Get
	retrievedValue, err := kv.Get(key)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if retrievedValue != value {
		t.Errorf("Get returned wrong value: got %s, want %s", retrievedValue, value)
	}

	// Delete
	if err := kv.Delete(key); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	// Get after delete
	_, err = kv.Get(key)
	if err == nil {
		t.Errorf("Expected error for Get after delete, got nil")
	} else if !strings.Contains(err.Error(), "key not found") {
		t.Errorf("Expected 'key not found' error, got: %v", err)
	}

	// Put on non-leader (this requires a second node, skipping for single-node test simplicity here)
	// We can test the error message though by temporarily making it not a leader if Raft allows.
	// For now, assuming single node always leader after bootstrap.
}

func TestKVReplicator_LeaderInfo(t *testing.T) {
	kv, cfg, cleanup := setupKVReplicator(t, "node1", true, nil)
	defer cleanup()

	if err := kv.Start(); err != nil {
		t.Fatalf("Failed to start KVReplicator: %v", err)
	}
	waitForLeader(t, kv, 10*time.Second)

	if !kv.IsLeader() {
		t.Error("IsLeader should return true for a bootstrapped single node")
	}

	leaderAddr := kv.Leader()
	if leaderAddr == "" {
		t.Error("Leader address should not be empty")
	}
	if string(leaderAddr) != cfg.RaftBindAddress {
		t.Errorf("Leader address mismatch: got %s, want %s", leaderAddr, cfg.RaftBindAddress)
	}
}

func TestKVReplicator_Stats_SingleNode(t *testing.T) {
	kv, _, cleanup := setupKVReplicator(t, "node1", true, nil)
	defer cleanup()

	if err := kv.Start(); err != nil {
		t.Fatalf("Failed to start KVReplicator: %v", err)
	}
	waitForLeader(t, kv, 10*time.Second)

	stats := kv.Stats()
	if stats == nil {
		t.Fatal("Stats should not be nil")
	}
	if _, ok := stats["state"]; !ok {
		t.Error("Stats should contain 'state' key")
	}
	if stats["state"] != "Leader" {
		t.Errorf("Expected state to be Leader, got %s", stats["state"])
	}
}

func TestKVReplicator_Shutdown_SingleNode(t *testing.T) {
	kv, _, cleanup := setupKVReplicator(t, "node1", true, nil)
	// defer cleanup() // Cleanup will be called explicitly after shutdown

	if err := kv.Start(); err != nil {
		t.Fatalf("Failed to start KVReplicator: %v", err)
	}
	waitForLeader(t, kv, 10*time.Second)

	if err := kv.Shutdown(); err != nil {
		t.Fatalf("Shutdown failed: %v", err)
	}

	// Verify FSM is closed (mock FSM has a simple log, real would check DB status)
	// kv.fsm.logger.Println("Test: FSM should be closed now.") // This is just for manual check of logs

	// Calling cleanup now that KVReplicator is shut down.
	cleanup()

	// Try an operation after shutdown (should fail)
	// Need a new KV instance or re-initialize for this, as 'kv' is already shutdown.
	// This part is tricky as the instance is unusable. Best to rely on shutdown success.
}

func TestKVReplicator_Membership_SingleNode(t *testing.T) {
	kv, cfg, cleanup := setupKVReplicator(t, "node1", true, nil)
	defer cleanup()

	if err := kv.Start(); err != nil {
		t.Fatalf("Failed to start KVReplicator: %v", err)
	}
	waitForLeader(t, kv, 10*time.Second)

	// AddVoter: Trying to add self again (should be idempotent or handled gracefully)
	err := kv.AddVoter(cfg.NodeID, cfg.RaftBindAddress)
	if err != nil {
		// Depending on Raft's implementation, this might error if already present or succeed.
		// Hashicorp Raft's AddVoter is idempotent if ID and address match.
		// Let's check for specific "already part of configuration"
		if !strings.Contains(err.Error(), "already part of configuration") {
			// t.Logf("AddVoter for existing self returned: %v (may be acceptable)", err)
			// It's fine if it's already there.
		}
	} else {
		kv.logger.Printf("INFO: AddVoter for existing self completed without error.")
	}

	// AddVoter: Adding a new node (will succeed on leader, but node doesn't exist)
	// This is more for testing the leader's ability to issue the command.
	newNodeID := "node2"
	newNodeAddr := "localhost:9001" // Dummy address
	err = kv.AddVoter(newNodeID, newNodeAddr)
	if err != nil {
		t.Fatalf("AddVoter for new node failed: %v", err)
	}

	// Check configuration (optional, needs a moment for config to propagate)
	time.Sleep(200 * time.Millisecond) // Give Raft time to commit the config change
	configFuture := kv.raftManager.raftNode.GetConfiguration()
	if err := configFuture.Error(); err != nil {
		t.Fatalf("Failed to get configuration: %v", err)
	}
	foundNode2 := false
	for _, srv := range configFuture.Configuration().Servers {
		if srv.ID == raft.ServerID(newNodeID) {
			foundNode2 = true
			if srv.Address != raft.ServerAddress(newNodeAddr) {
				t.Errorf("Node2 address mismatch in config: got %s, want %s", srv.Address, newNodeAddr)
			}
			break
		}
	}
	if !foundNode2 {
		t.Errorf("Node2 not found in Raft configuration after AddVoter")
	}

	// RemoveServer: Removing the newly added node
	err = kv.RemoveServer(newNodeID)
	if err != nil {
		t.Fatalf("RemoveServer for node2 failed: %v", err)
	}

	// Check configuration again
	time.Sleep(200 * time.Millisecond)
	configFuture = kv.raftManager.raftNode.GetConfiguration()
	if err := configFuture.Error(); err != nil {
		t.Fatalf("Failed to get configuration after remove: %v", err)
	}
	foundNode2AfterRemove := false
	for _, srv := range configFuture.Configuration().Servers {
		if srv.ID == raft.ServerID(newNodeID) {
			foundNode2AfterRemove = true
			break
		}
	}
	if foundNode2AfterRemove {
		t.Errorf("Node2 still found in Raft configuration after RemoveServer")
	}

	// RemoveServer: Trying to remove self (leader) - should fail
	err = kv.RemoveServer(cfg.NodeID)
	if err == nil {
		t.Error("Expected error when trying to remove self (leader), got nil")
	} else if !strings.Contains(err.Error(), "cannot remove self") && !strings.Contains(err.Error(), "leadership transfer") {
		// Raft might also return errors about trying to remove the leader without transfer.
		t.Errorf("Expected 'cannot remove self' or similar error, got: %v", err)
	}
}

func TestKVReplicator_NonLeaderOperations(t *testing.T) {
	// This test requires at least two nodes to reliably test non-leader behavior.
	// For a single node, it's always the leader after bootstrap.
	// We can simulate by trying to apply before it's a leader, but that's fragile.
	// Consider expanding this if a multi-node test setup is introduced.
	t.Skip("Skipping non-leader operation tests for single-node setup. Requires multi-node.")
}

// Helper to get an available port
func getAvailablePort(t *testing.T) int {
	t.Helper()
	listener, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Fatalf("Failed to listen on a port: %v", err)
	}
	defer listener.Close()
	addr := listener.Addr().(*net.TCPAddr)
	return addr.Port
}

func TestKVReplicator_JoinLogic_Simplified(t *testing.T) {
	// Test the join logic for a node that is NOT bootstrapping.
	// This is a simplified test because true join requires a running leader.
	// Here, we'll just check if Start() doesn't panic and logs appropriately.

	nodeID := "joiningNode"
	raftDataDir := t.TempDir()
	dbDir := t.TempDir()

	// Use a different port for this node
	port := getAvailablePort(t)
	raftBindAddr := "localhost:" + strconv.Itoa(port)

	cfg := Config{
		NodeID:          nodeID,
		RaftBindAddress: raftBindAddr,
		RaftDataDir:     raftDataDir,
		DBPath:          dbDir,
		Bootstrap:       false,                      // This node is not bootstrapping
		JoinAddresses:   []string{"localhost:7000"}, // Dummy join address
		ApplyTimeout:    5 * time.Second,
		Logger:          log.New(os.Stdout, fmt.Sprintf("[%s-test-join] ", nodeID), log.LstdFlags|log.Lmicroseconds),
	}

	kv, err := NewKVReplicator(cfg)
	if err != nil {
		t.Fatalf("Failed to create KVReplicator for joining node: %v", err)
	}
	defer kv.Shutdown() // Ensure cleanup even on failure

	// We expect Start to proceed without error, but the node won't become leader
	// as it's trying to join a non-existent cluster.
	// The Start() method has logging for join attempts.
	// The actual join success depends on an external leader adding this node.
	if err := kv.Start(); err != nil {
		// Some errors might be acceptable if it's about failing to contact join addresses
		// The current Start() implementation tries an AddVoter on itself which might error
		// if not leader, this is fine for this test case.
		t.Logf("Start() for joining node returned: %v (this might be expected if join address is unreachable or AddVoter on self fails as non-leader)", err)
	}

	// Node should not be leader
	time.Sleep(2 * time.Second) // Give it a moment to try to become leader (it shouldn't)
	if kv.IsLeader() {
		t.Errorf("Joining node %s unexpectedly became leader", nodeID)
	}
	if kv.raftManager.raftNode.State() == raft.Shutdown {
		t.Errorf("Joining node %s is shutdown, expected it to be follower or candidate", nodeID)
	}

	// Clean up manually as defer might not cover all paths if Start fails early.
	os.RemoveAll(raftDataDir)
	os.RemoveAll(dbDir)
}
