package wal_replicator

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/cockroachdb/pebble"
	// Import ZooKeeper client
)

// WALReplicationServer provides a server for a key-value store using a local PebbleDB instance.
// Note: This implementation currently acts as a local key-value store and does NOT include
// Write-Ahead Logging (WAL) or replication logic. The membership/status endpoints are placeholders.
type WALReplicationServer struct {
	config      WALConfig
	logger      *log.Logger
	db          *pebble.DB   // Add PebbleDB instance
	httpServer  *http.Server // Add HTTP server instance for graceful shutdown
	zkManager   *ZKManager   // Add ZKManager instance for ZooKeeper operations
	activeNodes map[string]string
	mu          sync.RWMutex // Mutex to protect activeNodes
}

// WALConfig is configuration for the WAL replication server.
type WALConfig struct {
	NodeID              string
	InternalBindAddress string   // Address for this node to listen on for internal communication (e.g., replication, gossip)
	HTTPAddr            string   // Address for the HTTP API server to listen on (e.g., ":8080")
	DataDir             string   // Directory for WAL files, DB, etc. (used for PebbleDB path)
	ZkServers           []string // Addresses of ZooKeeper servers (e.g., ["localhost:2181"])
	// Other WAL specific configuration parameters
}

// NewWALReplicationServer creates and initializes a new WALReplicationServer instance.
func NewWALReplicationServer(cfg WALConfig) (*WALReplicationServer, error) {
	logger := log.New(os.Stdout, fmt.Sprintf("[%s-wal] ", cfg.NodeID), log.LstdFlags|log.Lmicroseconds)

	logger.Printf("Initializing WALReplicationServer for node %s with data dir %s",
		cfg.NodeID, cfg.DataDir)

	if cfg.DataDir == "" {
		return nil, fmt.Errorf("WALConfig error: DataDir must be specified")
	}

	// Ensure data directory exists
	if err := os.MkdirAll(cfg.DataDir, 0755); err != nil {
		logger.Printf("ERROR: Failed to create data directory %s: %v", cfg.DataDir, err)
		return nil, fmt.Errorf("failed to create data directory: %w", err)
	}

	// Open PebbleDB
	opts := &pebble.Options{} // Use default options for now
	db, err := pebble.Open(cfg.DataDir, opts)
	if err != nil {
		logger.Printf("ERROR: Failed to open Pebble DB at %s: %v", cfg.DataDir, err)
		return nil, fmt.Errorf("failed to open pebble db: %w", err)
	}
	logger.Printf("Pebble DB opened successfully at %s", cfg.DataDir)

	server := &WALReplicationServer{
		config:      cfg,
		logger:      logger,
		db:          db,                      // Assign the opened DB
		activeNodes: make(map[string]string), // Initialize the activeNodes map
		mu:          sync.RWMutex{},          // Initialize the mutex
	}

	// Initialize ZKManager and connect
	server.zkManager = NewZKManager(logger, cfg.NodeID) // Initialize ZKManager without connection
	if err := server.zkManager.Connect(cfg.ZkServers); err != nil {
		db.Close() // Ensure DB is closed if ZK connection fails
		return nil, err
	}

	logger.Println("WALReplicationServer instance created successfully.")
	return server, nil
}

// Start initializes the WALReplicationServer and starts its HTTP API server.
// It does NOT start actual WAL replication logic yet.
func (wrs *WALReplicationServer) Start() error {
	wrs.logger.Printf("WALReplicationServer node %s starting...", wrs.config.NodeID)

	// --- Placeholder Start Logic ---
	// Simulate starting internal WAL components (None implemented yet)
	wrs.logger.Println("Placeholder WAL internal components started.")
	// --- End Placeholder Start Logic ---

	// Start ZKManager, which handles node registration and sets up event watches
	if wrs.zkManager != nil {
		if err := wrs.zkManager.Start(wrs.config.InternalBindAddress); err != nil {
			wrs.logger.Printf("Error starting ZKManager: %v", err)
			// Decide if startup should fail here or continue with a warning
		}
	}

	wrs.logger.Printf("WALReplicationServer node %s started successfully.", wrs.config.NodeID)
	wrs.logger.Printf("Internal address (placeholder): %s", wrs.config.InternalBindAddress)
	wrs.logger.Printf("HTTP API listening on: %s", wrs.config.HTTPAddr)

	// Setup HTTP server for API
	mux := wrs.setupWALHTTPServerMux()

	wrs.httpServer = &http.Server{
		Addr:    wrs.config.HTTPAddr,
		Handler: mux,
		// Add read/write timeouts for production use
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  120 * time.Second,
	}

	go func() {
		wrs.logger.Printf("Placeholder WAL HTTP server listening on %s", wrs.config.HTTPAddr)
		if err := wrs.httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			wrs.logger.Fatalf("Placeholder WAL HTTP server ListenAndServe: %v", err)
		}
	}()

	return nil
}

// Shutdown gracefully shuts down the WALReplicationServer, including the HTTP server and PebbleDB.
func (wrs *WALReplicationServer) Shutdown() error {
	wrs.logger.Println("Shutting down WALReplicationServer placeholder...")

	// Shutdown HTTP server
	if wrs.httpServer != nil {
		wrs.logger.Println("Shutting down HTTP server...")
		// Create a context with a timeout for the shutdown
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := wrs.httpServer.Shutdown(shutdownCtx); err != nil {
			wrs.logger.Printf("ERROR: HTTP server shutdown error: %v", err)
			// Continue, but report the error
		} else {
			wrs.logger.Println("HTTP server shut down.")
		}
		wrs.httpServer = nil
	}

	// Close ZooKeeper connection via ZKManager
	if wrs.zkManager != nil {
		wrs.zkManager.Close()
	}

	// --- Placeholder Shutdown Logic ---
	// Simulate shutting down internal WAL components (None implemented yet)
	wrs.logger.Println("Placeholder WAL internal components shut down.")
	// --- End Placeholder Shutdown Logic ---

	// Close PebbleDB
	if err := wrs.Close(); err != nil {
		wrs.logger.Printf("ERROR: Failed to close Pebble DB during shutdown: %v", err)
		// Continue with other shutdown steps if possible, but report the error
	}

	wrs.logger.Println("WALReplicationServer placeholder shut down successfully.")
	return nil
}

// Close closes the underlying PebbleDB database.
func (wrs *WALReplicationServer) Close() error {
	wrs.logger.Println("Closing Pebble DB...")
	if wrs.db != nil {
		err := wrs.db.Close()
		if err != nil {
			wrs.logger.Printf("ERROR: failed to close Pebble DB: %v", err)
			return err
		}
		wrs.logger.Println("Pebble DB closed.")
		wrs.db = nil
	} else {
		wrs.logger.Println("Close called, but Pebble DB was already nil.")
	}
	return nil
}

// Get retrieves a value for a given key from the local PebbleDB.
func (wrs *WALReplicationServer) Get(key string) (string, error) {
	wrs.logger.Printf("DB Get: key=%s", key) // Using Printf for potentially noisy ops
	if wrs.db == nil {
		return "", fmt.Errorf("pebble db is not initialized")
	}

	valueBytes, closer, err := wrs.db.Get([]byte(key))
	if err != nil {
		if err == pebble.ErrNotFound {
			wrs.logger.Printf("Key not found: %s", key)
			return "", fmt.Errorf("key not found: %s", key)
		}
		wrs.logger.Printf("ERROR: Pebble Get failed for key %s: %v", key, err)
		return "", fmt.Errorf("pebble get failed for key %s: %w", key, err)
	}
	defer closer.Close()

	// Make a copy of the valueBytes as it's only valid until closer.Close()
	valueStr := string(valueBytes)
	wrs.logger.Printf("DB Get successful: key=%s", key)
	return valueStr, nil
}

// Put sets a value for a given key in the local PebbleDB.
// Note: This operation is NOT replicated to other nodes in this placeholder implementation.
func (wrs *WALReplicationServer) Put(key, value string) error {
	wrs.logger.Printf("DB Put: key=%s, value=%s", key, value) // Using Printf for potentially noisy ops
	if wrs.db == nil {
		return fmt.Errorf("pebble db is not initialized")
	}

	writeOpts := pebble.Sync // Ensure data is flushed to disk
	err := wrs.db.Set([]byte(key), []byte(value), writeOpts)
	if err != nil {
		wrs.logger.Printf("ERROR: Pebble Set failed for Key=%s: %v", key, err)
		return fmt.Errorf("pebble set failed for key %s: %w", key, err)
	}
	wrs.logger.Printf("Successfully set key %s", key)
	return nil
}

// Delete removes a key from the local PebbleDB.
// Note: This operation is NOT replicated to other nodes in this placeholder implementation.
func (wrs *WALReplicationServer) Delete(key string) error {
	wrs.logger.Printf("DB Delete: key=%s", key) // Using Printf for potentially noisy ops
	if wrs.db == nil {
		return fmt.Errorf("pebble db is not initialized")
	}

	writeOpts := pebble.Sync // Ensure data is flushed to disk
	err := wrs.db.Delete([]byte(key), writeOpts)
	if err != nil {
		// Pebble's Delete doesn't error if key not found, it's a successful deletion of nothing.
		wrs.logger.Printf("ERROR: Pebble Delete failed for Key=%s: %v", key, err)
		return fmt.Errorf("pebble delete failed for key %s: %w", key, err)
	}
	wrs.logger.Printf("Successfully deleted key %s", key)
	// Pebble's Delete doesn't error if key not found, it's a successful deletion of nothing.
	wrs.logger.Printf("DB Delete successful: key=%s", key)
	return nil
}

// AddNode adds a node to the server's list of active nodes.
// This is used for explicit or administrative control over cluster membership.
func (wrs *WALReplicationServer) AddNode(nodeID string, nodeAddr string) error {
	wrs.mu.Lock()
	defer wrs.mu.Unlock()

	wrs.activeNodes[nodeID] = nodeAddr
	wrs.logger.Printf("Node added to active list: ID=%s, Address=%s. Current active nodes: %v", nodeID, nodeAddr, wrs.activeNodes)
	return nil
}

// RemoveNode removes a node from the server's list of active nodes.
// This is used for explicit or administrative control over cluster membership.
func (wrs *WALReplicationServer) RemoveNode(nodeID string) error {
	wrs.mu.Lock()
	defer wrs.mu.Unlock()

	if _, exists := wrs.activeNodes[nodeID]; exists {
		delete(wrs.activeNodes, nodeID)
		wrs.logger.Printf("Node removed from active list: ID=%s. Current active nodes: %v", nodeID, wrs.activeNodes)
	} else {
		wrs.logger.Printf("Attempted to remove non-existent node: ID=%s", nodeID)
	}
	return nil
}

// Placeholder method for IsPrimary/IsReplica
func (wrs *WALReplicationServer) IsPrimary() bool {
	// In a real implementation, this would return true if this node is the primary
	wrs.logger.Println("Received placeholder IsPrimary request.")
	return false // Placeholder: Assume not primary
}

// Placeholder method for GetPrimary
func (wrs *WALReplicationServer) GetPrimary() string {
	// In a real implementation, this would return the primary's address
	wrs.logger.Println("Received placeholder GetPrimary request.")
	return "placeholder-primary-address:xxxx (WAL replication not implemented)"
}

// Placeholder method for GetStats
func (wrs *WALReplicationServer) GetStats() map[string]string {
	wrs.logger.Println("Received placeholder GetStats request.")
	// In a real implementation, this would return WAL/replication stats
	return map[string]string{
		"status":             "placeholder - WAL replication not implemented",
		"replication_status": "not implemented",
		"node_id":            wrs.config.NodeID,
		"data_dir":           wrs.config.DataDir,
	}
}

// --- HTTP Handler Methods ---

// handleKV is the HTTP handler for /kv requests.
func (wrs *WALReplicationServer) handleKV(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Query().Get("key")
	if key == "" {
		http.Error(w, "key parameter is required", http.StatusBadRequest)
		return
	}

	switch r.Method {
	case http.MethodGet:
		value, err := wrs.Get(key)
		if err != nil {
			// Differentiate between not found and other errors
			if strings.Contains(err.Error(), "key not found") {
				http.Error(w, err.Error(), http.StatusNotFound)
			} else {
				http.Error(w, fmt.Sprintf("Failed to get key from local DB: %v", err), http.StatusInternalServerError)
			}
			return
		}
		fmt.Fprint(w, value)
	case http.MethodPut:
		value := r.URL.Query().Get("value")
		if value == "" {
			http.Error(w, "value parameter is required for PUT", http.StatusBadRequest)
			return
		}
		// Call the local Put method
		err := wrs.Put(key, value)
		if err != nil {
			http.Error(w, fmt.Sprintf("Failed to put key to local DB: %v", err), http.StatusInternalServerError)
			return
		}
		fmt.Fprint(w, "OK (Operation applied to local DB)")
	case http.MethodDelete:
		// Call the local Delete method
		err := wrs.Delete(key)
		if err != nil {
			http.Error(w, fmt.Sprintf("Failed to delete key from local DB: %v", err), http.StatusInternalServerError)
			return
		}
		fmt.Fprint(w, "OK (Operation applied to local DB)")
	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

// handleWALJoin is the HTTP handler for /wal/join requests (Placeholder).
func (wrs *WALReplicationServer) handleWALJoin(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST only", http.StatusMethodNotAllowed)
		return
	}
	nodeID := r.URL.Query().Get("nodeId")
	nodeAddr := r.URL.Query().Get("address")
	if nodeID == "" || nodeAddr == "" {
		http.Error(w, "nodeId and address parameters are required", http.StatusBadRequest)
		return
	}
	err := wrs.AddNode(nodeID, nodeAddr)
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to add node: %v", err), http.StatusInternalServerError)
		return
	}
	fmt.Fprintf(w, "Node %s (%s) added successfully to active list.", nodeID, nodeAddr)
}

// handleWALRemove is the HTTP handler for /wal/remove requests (Placeholder).
func (wrs *WALReplicationServer) handleWALRemove(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST only", http.StatusMethodNotAllowed)
		return
	}
	nodeID := r.URL.Query().Get("nodeId")
	if nodeID == "" {
		http.Error(w, "nodeId parameter is required", http.StatusBadRequest)
		return
	}
	err := wrs.RemoveNode(nodeID)
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to remove node: %v", err), http.StatusInternalServerError)
		return
	}
	fmt.Fprintf(w, "Node %s removed successfully from active list.", nodeID)
}

// handleWALPrimary is the HTTP handler for /wal/primary requests (Placeholder).
func (wrs *WALReplicationServer) handleWALPrimary(w http.ResponseWriter, r *http.Request) {
	primary := wrs.GetPrimary() // Calls placeholder
	fmt.Fprintf(w, "Placeholder Primary address: %s\n", primary)
	fmt.Fprintf(w, "Placeholder: Is this node primary: %t (WAL replication not implemented)\n", wrs.IsPrimary()) // Calls placeholder
}

// handleWALStats is the HTTP handler for /wal/stats requests (Placeholder).
func (wrs *WALReplicationServer) handleWALStats(w http.ResponseWriter, r *http.Request) {
	stats := wrs.GetStats() // Calls placeholder
	for k, v := range stats {
		fmt.Fprintf(w, "%s: %s\n", k, v)
	}
}

// setupWALHTTPServerMux sets up the HTTP request multiplexer for the WAL replicator placeholder.
// It registers all the handler functions.
func (wrs *WALReplicationServer) setupWALHTTPServerMux() *http.ServeMux {
	mux := http.NewServeMux()

	// Register KV Endpoints
	mux.HandleFunc("/kv", wrs.handleKV)

	// Register Membership Endpoints (Placeholder)
	mux.HandleFunc("/wal/join", wrs.handleWALJoin)
	mux.HandleFunc("/wal/remove", wrs.handleWALRemove)

	// Register Status Endpoints (Placeholder)
	mux.HandleFunc("/wal/primary", wrs.handleWALPrimary)
	mux.HandleFunc("/wal/stats", wrs.handleWALStats)

	return mux
}
