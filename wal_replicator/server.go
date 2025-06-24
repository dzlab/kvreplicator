package wal_replicator

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/go-zookeeper/zk" // Import ZooKeeper client
)

// WALReplicationServer provides a server for a key-value store using a local PebbleDB instance.
// Note: This implementation currently acts as a local key-value store and does NOT include
// Write-Ahead Logging (WAL) or replication logic. The membership/status endpoints are placeholders.
type WALReplicationServer struct {
	config     WALConfig
	logger     *log.Logger
	db         *pebble.DB   // Add PebbleDB instance
	httpServer *http.Server // Add HTTP server instance for graceful shutdown
	zkConn     *zk.Conn     // Add ZooKeeper connection
	zkManager  *ZKManager   // Add ZKManager instance for ZooKeeper operations
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
		config: cfg,
		logger: logger,
		db:     db,  // Assign the opened DB
		zkConn: nil, // Initialize zkConn to nil
	}

	// Connect to ZooKeeper
	if len(cfg.ZkServers) == 0 {
		logger.Println("WARNING: No ZooKeeper servers specified in config. Membership features will not work.")
	} else {
		logger.Printf("Connecting to ZooKeeper at %v...", cfg.ZkServers)
		conn, _, err := zk.Connect(cfg.ZkServers, time.Second*10) // 10-second timeout for connection
		if err != nil {
			// Close DB if ZK connection fails, to clean up
			db.Close()
			logger.Printf("ERROR: Failed to connect to ZooKeeper: %v", err)
			return nil, fmt.Errorf("failed to connect to zookeeper: %w", err)
		}
		logger.Println("ZooKeeper connection established.")
		// Basic check: ensure root path exists
		exists, _, err := conn.Exists("/kvreplicator/wal")
		if err != nil {
			conn.Close()
			db.Close()
			logger.Printf("ERROR: Failed to check existence of ZK path /kvreplicator/wal: %v", err)
			return nil, fmt.Errorf("zookeeper path check failed: %w", err)
		}
		if !exists {
			logger.Println("ZK path /kvreplicator/wal does not exist, creating...")
			_, err = conn.Create("/kvreplicator/wal", nil, 0, zk.WorldACL(zk.PermAll))
			if err != nil && err != zk.ErrNodeExists { // ErrNodeExists is ok
				conn.Close()
				db.Close()
				logger.Printf("ERROR: Failed to create ZK path /kvreplicator/wal: %v", err)
				return nil, fmt.Errorf("failed to create zookeeper path: %w", err)
			}
			logger.Println("ZK path /kvreplicator/wal created or already exists.")
		}
		// Assign the successful ZK connection to the server instance
		server.zkConn = conn
		server.zkManager = NewZKManager(conn, logger, cfg.NodeID) // Initialize ZKManager
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

	// Register node in ZooKeeper and set up watches
	if wrs.zkManager != nil {
		err := wrs.zkManager.RegisterNode(wrs.config.InternalBindAddress)
		if err != nil {
			wrs.logger.Printf("Error during ZK node registration: %v", err)
			// Decide if startup should fail here or continue with a warning
		}

		eventChan, err := wrs.zkManager.EnsureNodesPathExists()
		if err != nil {
			wrs.logger.Printf("Error ensuring ZK nodes path exists or setting watch: %v", err)
			// Decide if startup should fail here or continue with a warning
		} else if eventChan != nil {
			// Start goroutine to handle ZooKeeper events
			go wrs.handleZkEvents(eventChan) // Pass the event channel to a new handler function
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

	// Close ZooKeeper connection
	if wrs.zkConn != nil {
		wrs.logger.Println("Closing ZooKeeper connection...")
		wrs.zkConn.Close()
		wrs.logger.Println("ZooKeeper connection closed.")
		wrs.zkConn = nil
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

// Placeholder method for AddNode (WAL equivalent of AddVoter/AddLearner)
func (wrs *WALReplicationServer) AddNode(nodeID string, nodeAddr string) error {
	wrs.logger.Printf("Received placeholder AddNode request: ID=%s, Address=%s", nodeID, nodeAddr)
	// In a real implementation, this would add the node to a configuration/membership list
	return fmt.Errorf("AddNode operation not implemented for WAL replicator yet")
}

// Placeholder method for RemoveNode
func (wrs *WALReplicationServer) RemoveNode(nodeID string) error {
	wrs.logger.Printf("Received placeholder RemoveNode request: ID=%s", nodeID)
	// In a real implementation, this would remove the node from the membership list
	return fmt.Errorf("RemoveNode operation not implemented for WAL replicator yet")
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
	// Call placeholder AddNode
	err := wrs.AddNode(nodeID, nodeAddr) // This will return the "not implemented" error
	http.Error(w, fmt.Sprintf("Placeholder AddNode failed: %v", err), http.StatusNotImplemented)
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
	// Call placeholder RemoveNode
	err := wrs.RemoveNode(nodeID) // This will return the "not implemented" error
	http.Error(w, fmt.Sprintf("Placeholder RemoveNode failed: %v", err), http.StatusNotImplemented)
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

// handleZkEvents processes events received from the ZooKeeper event channel.
// This function is intended to run in a separate goroutine.
func (wrs *WALReplicationServer) handleZkEvents(initialEventChan <-chan zk.Event) {
	wrs.logger.Println("Starting ZooKeeper event handler goroutine.")

	// Use a variable to hold the current event channel
	currentEventChan := initialEventChan

	for {
		select {
		case event, ok := <-currentEventChan:
			if !ok {
				wrs.logger.Println("ZooKeeper event channel closed. Stopping event handler.")
				return // Exit the goroutine if the channel is closed
			}
			wrs.logger.Printf("Received ZK event: %+v", event)
			switch event.Type {
			case zk.EventSession:
				// Handle session state changes (e.g., connected, disconnected, expired)
				wrs.logger.Printf("ZooKeeper session event: State -> %s, Server -> %s", event.State.String(), event.Server)
				// TODO: Implement logic for session state changes (e.g., re-register ephemeral node if session expires)
				if event.State == zk.StateExpired || event.State == zk.StateDisconnected {
					// A disconnected or expired session means we've lost our ephemeral node registration.
					// Depending on the desired behavior, we might try to re-connect or rely on Start() to handle this.
					// For now, we'll just log it as a critical event.
					wrs.logger.Println("ZooKeeper session expired or disconnected. Node registration might be lost.")
				}
			case zk.EventNodeCreated:
				wrs.logger.Printf("ZooKeeper node created: Path -> %s", event.Path)
				// TODO: Implement logic for node creation events (e.g., watch data for new nodes)
				// You might want to set a data watch on the newly created node:
				// _, _, newEventChan, err := wrs.zkConn.GetW(event.Path)
				// if err == nil { // Handle error
				//   // Decide if you need to switch to this new channel or merge events
				//   // For data watches on many nodes, you'd typically have separate goroutines or fan-in channels
				// }
			case zk.EventNodeDeleted:
				wrs.logger.Printf("ZooKeeper node deleted: Path -> %s", event.Path)
				// TODO: Implement logic for node deletion events (e.g., remove from list of active nodes)
			case zk.EventNodeDataChanged:
				wrs.logger.Printf("ZooKeeper node data changed: Path -> %s", event.Path)
				// TODO: Implement logic for node data changes (e.g., primary node data)
				// If this is a data change on the primary election path, we might need to re-evaluate primary status.
				// After processing, you might need to re-set the data watch on the path:
				// _, _, newEventChan, err := wrs.zkConn.GetW(event.Path)
				// if err == nil { // Handle error
				//   // Decide if you need to switch to this new channel or merge events
				// }
			case zk.EventNodeChildrenChanged:
				wrs.logger.Printf("ZooKeeper node children changed: Path -> %s", event.Path)
				// This happens when nodes are added/removed under the watched path (/kvreplicator/wal/nodes)
				// We need to re-get the children and set a new watch on the *same* path.
				children, _, newEventChan, err := wrs.zkConn.ChildrenW(event.Path)
				if err != nil {
					wrs.logger.Printf("ERROR: Failed to re-set watch or get children for path %s after children changed event: %v", event.Path, err)
					// Depending on error handling, you might want to stop the goroutine or try again.
					// For now, we log the error and continue trying to read from the old channel.
				} else {
					wrs.logger.Printf("Re-set watch on %s. Current children: %v", event.Path, children)
					// Successfully re-set the watch, switch to the new event channel.
					currentEventChan = newEventChan
					// TODO: Implement logic to update the internal list of active nodes based on `children`.
					// This list is crucial for replication/failover logic.
				}
			default:
				wrs.logger.Printf("Received unhandled ZooKeeper event type: %s (Type: %d, Path: %s, Err: %v)", event.Type.String(), event.Type, event.Path, event.Err)
			}
		}
	}
}
