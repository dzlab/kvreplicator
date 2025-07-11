package wal_replicator

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"strings"
	"time"
)

// WALReplicationServer provides a server for a key-value store using a local PebbleDB instance.
type WALReplicationServer struct {
	config     WALConfig
	logger     *log.Logger
	db         *PebbleDBStore // Use the new PebbleDBStore type
	httpServer *http.Server   // Add HTTP server instance for graceful shutdown
	zkManager  *ZKManager     // Add ZKManager instance for ZooKeeper operations
	replicator *Replicator    // Add Replicator instance for WAL push replication
}

// WALConfig holds configuration parameters for the WALReplicationServer.
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

	// Initialize PebbleDBStore
	dbStore, err := NewPebbleDBStore(cfg.DataDir, logger)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize pebble db store: %w", err)
	}

	server := &WALReplicationServer{
		config: cfg,
		logger: logger,
		db:     dbStore, // Assign the new PebbleDBStore
	}

	// Define the node change callback to trigger replicator reconciliation
	nodeChangeCallback := func(activeNodes map[string]string) {
		server.logger.Printf("Node change detected by WALReplicationServer. Triggering replicator reconciliation.")
		if server.replicator != nil {
			server.replicator.TriggerReconciliation()
		}
	}

	// Initialize ZKManager and connect, passing the node change callback
	server.zkManager = NewZKManager(logger, cfg.NodeID, nodeChangeCallback)
	if err := server.zkManager.Connect(cfg.ZkServers); err != nil {
		dbStore.Close() // Ensure DB is closed if ZK connection fails
		return nil, err
	}

	// Initialize the Replicator
	server.replicator = NewReplicator(
		logger,
		cfg.NodeID,
		server,           // WALReplicationServer implements PrimaryChecker (IsPrimary, GetPrimary)
		server.zkManager, // ZKManager implements NodeRegistry (GetActiveNodes)
		server.db,        // PebbleDBStore implements WALSource (GetLatestSequenceNumber, GetUpdatesSince)
	)

	logger.Println("WALReplicationServer instance created successfully.")
	return server, nil
}

// Start initializes the WALReplicationServer and starts its HTTP API server.
// It does NOT start actual WAL replication logic yet.
func (wrs *WALReplicationServer) Start() error {
	wrs.logger.Printf("WALReplicationServer node %s starting...", wrs.config.NodeID)

	// Start ZKManager, which handles node registration, primary election, and sets up event watches
	if wrs.zkManager != nil {
		if err := wrs.zkManager.Start(wrs.config.InternalBindAddress); err != nil {
			wrs.logger.Printf("Error starting ZKManager: %v", err)
			return err // Fail if ZKManager cannot start properly
		}
	}

	// Start the Replicator if initialized
	if wrs.replicator != nil {
		wrs.replicator.Start()
	}

	// Register the WALReplicationServer as an RPC service
	// This makes its methods callable via RPC, specifically for replication.
	// We are using http.DefaultServeMux for RPC, which allows RPC and HTTP API to coexist
	// on the same port, as `net/rpc/server` registers handlers on http.DefaultServeMux.
	if err := rpc.Register(wrs); err != nil {
		return fmt.Errorf("failed to register WALReplicationServer RPC: %w", err)
	}
	rpc.HandleHTTP() // This registers /_goRPC_ and /debug/rpc

	wrs.logger.Printf("WALReplicationServer node %s started successfully.", wrs.config.NodeID)
	wrs.logger.Printf("Internal address: %s", wrs.config.InternalBindAddress)
	wrs.logger.Printf("HTTP API listening on: %s", wrs.config.HTTPAddr)
	wrs.logger.Printf("RPC service listening on %s (via HTTP)", wrs.config.HTTPAddr)

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
		wrs.logger.Printf("WAL HTTP server listening on %s", wrs.config.HTTPAddr)
		if err := wrs.httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			wrs.logger.Fatalf("WAL HTTP server ListenAndServe: %v", err)
		}
	}()

	return nil
}

// Shutdown gracefully shuts down the WALReplicationServer, including the HTTP server and PebbleDB.
func (wrs *WALReplicationServer) Shutdown() error {
	wrs.logger.Println("Shutting down WALReplicationServer...")

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

	// Stop the Replicator
	if wrs.replicator != nil {
		wrs.replicator.Stop()
	}

	// Close ZooKeeper connection via ZKManager
	if wrs.zkManager != nil {
		wrs.zkManager.Close()
	}

	// Close PebbleDB
	if err := wrs.Close(); err != nil {
		wrs.logger.Printf("ERROR: Failed to close Pebble DB during shutdown: %v", err)
		// Continue with other shutdown steps if possible, but report the error
	}

	wrs.logger.Println("WALReplicationServer shut down successfully.")
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
	valueStr, err := wrs.db.Get(key)
	if err != nil {
		return "", err
	}
	wrs.logger.Printf("DB Get successful: key=%s", key)
	return valueStr, nil
}

// Put sets a value for a given key in the local PebbleDB.
// If this node is the primary, it waits for the write to be committed by a quorum.
func (wrs *WALReplicationServer) Put(key, value string) error {
	wrs.logger.Printf("DB Put: key=%s, value=%s", key, value)
	err := wrs.db.Put(key, value)
	if err != nil {
		return err
	}

	if wrs.IsPrimary() && wrs.replicator != nil {
		latestLocalSeq, err := wrs.db.GetLatestSequenceNumber()
		if err != nil {
			wrs.logger.Printf("WARNING: Failed to get latest sequence number after local Put: %v", err)
			return fmt.Errorf("local put successful but failed to get latest seq: %w", err)
		}
		wrs.logger.Printf("Primary node local Put successful, waiting for quorum commit for SeqNum %d...", latestLocalSeq)
		if err := wrs.waitForQuorumCommit(latestLocalSeq); err != nil {
			wrs.logger.Printf("ERROR: Quorum commit failed for Put operation (key=%s, seq=%d): %v", key, latestLocalSeq, err)
			return fmt.Errorf("put operation failed to reach quorum commit: %w", err)
		}
		wrs.logger.Printf("Primary node Put operation (key=%s, seq=%d) committed by quorum.", key, latestLocalSeq)
	} else {
		wrs.logger.Printf("Successfully set key %s (not primary or replicator not initialized, no quorum wait).", key)
	}
	return nil
}

// Delete removes a key from the local PebbleDB.
// If this node is the primary, it waits for the delete to be committed by a quorum.
func (wrs *WALReplicationServer) Delete(key string) error {
	wrs.logger.Printf("DB Delete: key=%s", key)
	err := wrs.db.Delete(key)
	if err != nil {
		return err
	}

	if wrs.IsPrimary() && wrs.replicator != nil {
		latestLocalSeq, err := wrs.db.GetLatestSequenceNumber()
		if err != nil {
			wrs.logger.Printf("WARNING: Failed to get latest sequence number after local Delete: %v", err)
			return fmt.Errorf("local delete successful but failed to get latest seq: %w", err)
		}
		wrs.logger.Printf("Primary node local Delete successful, waiting for quorum commit for SeqNum %d...", latestLocalSeq)
		if err := wrs.waitForQuorumCommit(latestLocalSeq); err != nil {
			wrs.logger.Printf("ERROR: Quorum commit failed for Delete operation (key=%s, seq=%d): %v", key, latestLocalSeq, err)
			return fmt.Errorf("delete operation failed to reach quorum commit: %w", err)
		}
		wrs.logger.Printf("Primary node Delete operation (key=%s, seq=%d) committed by quorum.", key, latestLocalSeq)
	} else {
		wrs.logger.Printf("Successfully deleted key %s (not primary or replicator not initialized, no quorum wait).", key)
	}
	return nil
}

// IsPrimary checks if this node is currently the primary in the cluster.
func (wrs *WALReplicationServer) IsPrimary() bool {
	if wrs.zkManager == nil {
		wrs.logger.Println("ZKManager not initialized, cannot determine primary status.")
		return false
	}
	isPrimary, _, err := wrs.zkManager.GetPrimaryInfo()
	if err != nil {
		wrs.logger.Printf("ERROR: Failed to get primary info from ZK: %v", err)
		return false
	}
	return isPrimary
}

// GetPrimary returns the address of the current primary node.
func (wrs *WALReplicationServer) GetPrimary() string {
	if wrs.zkManager == nil {
		wrs.logger.Println("ZKManager not initialized, cannot determine primary address.")
		return "unknown (ZKManager not initialized)"
	}
	_, primaryAddr, err := wrs.zkManager.GetPrimaryInfo()
	if err != nil {
		wrs.logger.Printf("ERROR: Failed to get primary info from ZK: %v", err)
		return "unknown (error fetching primary address)"
	}
	return primaryAddr
}

// GetStats returns various statistics about the WALReplicationServer.
func (wrs *WALReplicationServer) GetStats() map[string]string {
	wrs.logger.Println("Received GetStats request.")
	// In a real implementation, this would return detailed WAL/replication stats.
	// For now, it provides basic node information and status.
	return map[string]string{
		"node_id":          wrs.config.NodeID,
		"status":           "running",
		"http_address":     wrs.config.HTTPAddr,
		"internal_address": wrs.config.InternalBindAddress,
		"data_dir":         wrs.config.DataDir,
		"is_primary":       fmt.Sprintf("%t", wrs.IsPrimary()),
		"current_primary":  wrs.GetPrimary(),
		// Add more detailed stats here as features are implemented
	}
}

// --- HTTP Handler Methods ---
// GetLatestSequenceNumber returns the latest sequence number from PebbleDB.
func (wrs *WALReplicationServer) GetLatestSequenceNumber() (uint64, error) {
	seqNum, err := wrs.db.GetLatestSequenceNumber()
	if err != nil {
		return 0, err
	}
	return seqNum, nil
}

// GetUpdatesSince scans the WAL files and returns all mutation events since a given sequence number.
func (wrs *WALReplicationServer) GetUpdatesSince(sinceSeq uint64) ([]WALUpdate, error) {
	updates, err := wrs.db.GetUpdatesSince(sinceSeq)
	if err != nil {
		return nil, err
	}
	return updates, nil
}

// ApplyWALUpdates is an RPC method that allows a primary node to push WAL updates to this follower.
func (wrs *WALReplicationServer) ApplyWALUpdates(args ApplyWALUpdatesArgs, reply *ApplyWALUpdatesReply) error {
	if wrs.IsPrimary() {
		reply.Success = false
		reply.Message = "This node is primary, cannot apply updates from another primary."
		return fmt.Errorf("node %s is primary, refusing to apply WAL updates from another primary", wrs.config.NodeID)
	}

	wrs.logger.Printf("Received %d WAL updates for application.", len(args.Updates))
	var lastAppliedSeq uint64 = 0

	for _, update := range args.Updates {
		var err error
		switch update.Op {
		case "put":
			err = wrs.Put(update.Key, update.Value)
		case "delete":
			err = wrs.Delete(update.Key)
		default:
			wrs.logger.Printf("WARNING: Unknown WAL operation type: %s for SeqNum %d", update.Op, update.SeqNum)
			continue // Skip unknown operations
		}

		if err != nil {
			wrs.logger.Printf("ERROR: Failed to apply WAL update %s for key %s (seq %d): %v", update.Op, update.Key, update.SeqNum, err)
			reply.Success = false
			reply.Message = fmt.Sprintf("Failed to apply update for seq %d: %v", update.SeqNum, err)
			// Return current lastAppliedSeq, indicating partial success up to this point
			if len(args.Updates) > 0 {
				reply.ReceivedSeqNum = args.Updates[len(args.Updates)-1].SeqNum
			}
			reply.AppliedSeqNum = lastAppliedSeq
			return fmt.Errorf("failed to apply WAL update: %w", err)
		}
		lastAppliedSeq = update.SeqNum
	}

	reply.Success = true
	reply.Message = "WAL updates applied successfully."
	// ReceivedSeqNum is the highest sequence number in the batch that was *received*.
	// This will be the last update in the batch if the batch is non-empty.
	if len(args.Updates) > 0 {
		reply.ReceivedSeqNum = args.Updates[len(args.Updates)-1].SeqNum
	}
	reply.AppliedSeqNum = lastAppliedSeq // AppliedSeqNum is the highest sequence number successfully *applied*.
	wrs.logger.Printf("Successfully applied %d WAL updates. Last received sequence: %d, Last applied sequence: %d",
		len(args.Updates), reply.ReceivedSeqNum, reply.AppliedSeqNum)
	return nil
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

// handleWALPrimary is the HTTP handler for /wal/primary requests.
func (wrs *WALReplicationServer) handleWALPrimary(w http.ResponseWriter, r *http.Request) {
	isPrimary := wrs.IsPrimary()
	primaryAddr := wrs.GetPrimary()
	fmt.Fprintf(w, "Is this node primary: %t\n", isPrimary)
	fmt.Fprintf(w, "Current Primary address: %s\n", primaryAddr)
}

// handleWALStats is the HTTP handler for /wal/stats requests.
func (wrs *WALReplicationServer) handleWALStats(w http.ResponseWriter, r *http.Request) {
	stats := wrs.GetStats() // Calls placeholder
	for k, v := range stats {
		fmt.Fprintf(w, "%s: %s\n", k, v)
	}
}

// handleWALSeqNum is the HTTP handler for /wal/seqnum requests.
func (wrs *WALReplicationServer) handleWALSeqNum(w http.ResponseWriter, r *http.Request) {
	seqNum, err := wrs.GetLatestSequenceNumber()
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to get latest sequence number: %v", err), http.StatusInternalServerError)
		return
	}
	fmt.Fprintf(w, "%d", seqNum)
}

// handleWALUpdates is the HTTP handler for /wal/updates requests.
func (wrs *WALReplicationServer) handleWALUpdates(w http.ResponseWriter, r *http.Request) {
	sinceStr := r.URL.Query().Get("since")
	if sinceStr == "" {
		http.Error(w, "query parameter 'since' is required", http.StatusBadRequest)
		return
	}

	sinceSeq, err := strconv.ParseUint(sinceStr, 10, 64)
	if err != nil {
		http.Error(w, "invalid 'since' parameter, must be a non-negative integer", http.StatusBadRequest)
		return
	}

	updates, err := wrs.GetUpdatesSince(sinceSeq)
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to get updates from WAL: %v", err), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(updates); err != nil {
		wrs.logger.Printf("ERROR: Failed to encode WAL updates to JSON: %v", err)
	}
}

// setupWALHTTPServerMux sets up the HTTP request multiplexer for the WAL replicator.
// It registers all the handler functions.
func (wrs *WALReplicationServer) setupWALHTTPServerMux() *http.ServeMux {
	mux := http.NewServeMux()

	// Register KV Endpoints
	mux.HandleFunc("/kv", wrs.handleKV)

	// Register Status Endpoints (relying on ZooKeeper for membership)
	mux.HandleFunc("/wal/primary", wrs.handleWALPrimary)
	mux.HandleFunc("/wal/stats", wrs.handleWALStats)
	mux.HandleFunc("/wal/updates", wrs.handleWALUpdates)
	mux.HandleFunc("/wal/seqnum", wrs.handleWALSeqNum)

	return mux
}

// waitForQuorumCommit waits until the given sequence number is committed by a quorum of nodes.
func (wrs *WALReplicationServer) waitForQuorumCommit(targetSeqNum uint64) error {
	const (
		pollInterval = 100 * time.Millisecond // How often to check for commit status
		timeout      = 5 * time.Second        // Max time to wait for a commit
	)

	if wrs.replicator == nil {
		return fmt.Errorf("replicator not initialized, cannot wait for quorum commit")
	}

	timer := time.NewTimer(timeout)
	defer timer.Stop()

	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			committedSeq := wrs.replicator.GetCommittedSequenceNumber()
			if committedSeq >= targetSeqNum {
				return nil // Target sequence number has been committed by quorum
			}
			wrs.logger.Printf("Waiting for SeqNum %d to be committed (current committed: %d)...", targetSeqNum, committedSeq)
		case <-timer.C:
			return fmt.Errorf("timeout waiting for sequence number %d to be committed by quorum", targetSeqNum)
		}
	}
}
