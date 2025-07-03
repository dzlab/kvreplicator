package wal_replicator

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/wal"
)

// WALReplicationServer provides a server for a key-value store using a local PebbleDB instance.
// Note: This implementation currently acts as a local key-value store and does NOT include
// Write-Ahead Logging (WAL) or replication logic. Cluster membership is managed by ZooKeeper.
type WALReplicationServer struct {
	config     WALConfig
	logger     *log.Logger
	db         *pebble.DB   // Add PebbleDB instance
	httpServer *http.Server // Add HTTP server instance for graceful shutdown
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

// WALUpdate represents a single operation recorded in the WAL.
type WALUpdate struct {
	SeqNum uint64 `json:"seqNum"`
	Op     string `json:"op"`
	Key    string `json:"key"`
	Value  string `json:"value,omitempty"`
}

// WALUpdate represents a single operation recorded in the WAL.
type WALUpdate struct {
	SeqNum uint64 `json:"seqNum"`
	Op     string `json:"op"`
	Key    string `json:"key"`
	Value  string `json:"value,omitempty"`
}

// WALUpdate represents a single operation recorded in the WAL.
type WALUpdate struct {
	SeqNum uint64
	Op     string // "put" or "delete"
	Key    string
	Value  string // Empty for "delete"
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
		db:     db, // Assign the opened DB
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

	// Start ZKManager, which handles node registration, primary election, and sets up event watches
	if wrs.zkManager != nil {
		if err := wrs.zkManager.Start(wrs.config.InternalBindAddress); err != nil {
			wrs.logger.Printf("Error starting ZKManager: %v", err)
			return err // Fail if ZKManager cannot start properly
		}
	}

	wrs.logger.Printf("WALReplicationServer node %s started successfully.", wrs.config.NodeID)
	wrs.logger.Printf("Internal address: %s", wrs.config.InternalBindAddress)
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
		wrs.logger.Printf("WAL HTTP server listening on %s", wrs.config.HTTPAddr)
		if err := wrs.httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			wrs.logger.Fatalf("WAL HTTP server ListenAndServe: %v", err)
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

// GetLatestSequenceNumber returns the latest sequence number from PebbleDB.
func (wrs *WALReplicationServer) GetLatestSequenceNumber() (uint64, error) {
	if wrs.db == nil {
		return 0, fmt.Errorf("pebble db is not initialized")
	}
	// A snapshot provides a consistent view of the DB at a point in time,
	// and its sequence number is the latest sequence number at that time.
	snap := wrs.db.NewSnapshot()
	defer snap.Close()
	return snap.SeqNum(), nil
}

// GetUpdatesSince scans the WAL files and returns all mutation events since a given sequence number.
func (wrs *WALReplicationServer) GetUpdatesSince(sinceSeq uint64) ([]WALUpdate, error) {
	if wrs.db == nil {
		return nil, fmt.Errorf("pebble db is not initialized")
	}

	walFiles, err := filepath.Glob(filepath.Join(wrs.config.DataDir, "*.log"))
	if err != nil {
		return nil, fmt.Errorf("failed to list WAL files: %w", err)
	}
	sort.Strings(walFiles)

	var updates []WALUpdate

	for _, walFile := range walFiles {
		f, err := os.Open(walFile)
		if err != nil {
			wrs.logger.Printf("WARNING: could not open WAL file %s: %v", walFile, err)
			continue
		}

		r := wal.NewReader(f, 0)
		for {
			rr, err := r.NextRecord()
			if err != nil {
				if err == io.EOF {
					break
				}
				// It's possible to hit partial records at the end of a file.
				// We can log this but continue to the next file.
				wrs.logger.Printf("WARNING: error reading next record from %s: %v", walFile, err)
				break
			}

			batchRepr, err := io.ReadAll(rr)
			if err != nil {
				wrs.logger.Printf("WARNING: could not read WAL record from %s: %v", walFile, err)
				continue
			}

			if len(batchRepr) < 12 { // Header size
				continue
			}

			// The batch representation starts with an 8-byte sequence number and 4-byte count.
			baseSeqNum := binary.LittleEndian.Uint64(batchRepr[:8])
			count := binary.LittleEndian.Uint32(batchRepr[8:12])

			// If the entire batch is before the sequence number we're interested in, skip it.
			if baseSeqNum+uint64(count) <= sinceSeq {
				continue
			}

			var batch pebble.Batch
			if err := batch.SetRepr(batchRepr); err != nil {
				wrs.logger.Printf("WARNING: could not decode batch from WAL file %s: %v", walFile, err)
				continue
			}

			iter, err := batch.NewIter(nil)
			if err != nil {
				wrs.logger.Printf("WARNING: could not create iterator for batch from WAL file %s: %v", walFile, err)
				continue
			}

			currentSeqNum := baseSeqNum
			for iter.First(); iter.Valid(); iter.Next() {
				if currentSeqNum > sinceSeq {
					var op string
					var value string

					switch iter.Kind() {
					case pebble.InternalKeyKindSet:
						op = "put"
						value = string(iter.Value())
					case pebble.InternalKeyKindDelete:
						op = "delete"
					default:
						// Skip other kinds of operations (e.g., Merge, LogData)
						currentSeqNum++
						continue
					}

					updates = append(updates, WALUpdate{
						SeqNum: currentSeqNum,
						Op:     op,
						Key:    string(iter.Key()),
						Value:  value,
					})
				}
				currentSeqNum++
			}
			iter.Close()
		}
		f.Close() // Close file at the end of processing it.
	}
	return updates, nil
}

// --- HTTP Handler Methods ---
// GetLatestSequenceNumber returns the latest sequence number from PebbleDB.
func (wrs *WALReplicationServer) GetLatestSequenceNumber() (uint64, error) {
	if wrs.db == nil {
		return 0, fmt.Errorf("pebble db is not initialized")
	}
	// A snapshot provides a consistent view of the DB at a point in time,
	// and its sequence number is the latest sequence number at that time.
	snap := wrs.db.NewSnapshot()
	defer snap.Close()
	return snap.SeqNum(), nil
}

// GetUpdatesSince scans the WAL files and returns all mutation events since a given sequence number.
func (wrs *WALReplicationServer) GetUpdatesSince(sinceSeq uint64) ([]WALUpdate, error) {
	if wrs.db == nil {
		return nil, fmt.Errorf("pebble db is not initialized")
	}

	walFiles, err := filepath.Glob(filepath.Join(wrs.config.DataDir, "*.log"))
	if err != nil {
		return nil, fmt.Errorf("failed to list WAL files: %w", err)
	}
	sort.Strings(walFiles)

	var updates []WALUpdate

	for _, walFile := range walFiles {
		f, err := os.Open(walFile)
		if err != nil {
			wrs.logger.Printf("WARNING: could not open WAL file %s: %v", walFile, err)
			continue
		}

		r := wal.NewReader(f, 0)
		for {
			rr, err := r.NextRecord()
			if err != nil {
				if err == io.EOF {
					break
				}
				wrs.logger.Printf("WARNING: error reading next record from %s: %v", walFile, err)
				break
			}

			batchRepr, err := io.ReadAll(rr)
			if err != nil {
				wrs.logger.Printf("WARNING: could not read WAL record from %s: %v", walFile, err)
				continue
			}

			if len(batchRepr) < 12 { // Header size
				continue
			}

			baseSeqNum := binary.LittleEndian.Uint64(batchRepr[:8])
			count := binary.LittleEndian.Uint32(batchRepr[8:12])

			if baseSeqNum+uint64(count) <= sinceSeq {
				continue
			}

			var batch pebble.Batch
			if err := batch.SetRepr(batchRepr); err != nil {
				wrs.logger.Printf("WARNING: could not decode batch from WAL file %s: %v", walFile, err)
				continue
			}

			iter, err := batch.NewIter(nil)
			if err != nil {
				wrs.logger.Printf("WARNING: could not create iterator for batch from WAL file %s: %v", walFile, err)
				continue
			}

			currentSeqNum := baseSeqNum
			for iter.First(); iter.Valid(); iter.Next() {
				if currentSeqNum > sinceSeq {
					var op string
					var value string

					switch iter.Kind() {
					case pebble.InternalKeyKindSet:
						op = "put"
						value = string(iter.Value())
					case pebble.InternalKeyKindDelete:
						op = "delete"
					default:
						currentSeqNum++
						continue
					}

					updates = append(updates, WALUpdate{
						SeqNum: currentSeqNum,
						Op:     op,
						Key:    string(iter.Key()),
						Value:  value,
					})
				}
				currentSeqNum++
			}
			iter.Close()
		}
		f.Close()
	}
	return updates, nil
}

// --- HTTP Handler Methods ---
// GetLatestSequenceNumber returns the latest sequence number from PebbleDB.
func (wrs *WALReplicationServer) GetLatestSequenceNumber() (uint64, error) {
	if wrs.db == nil {
		return 0, fmt.Errorf("pebble db is not initialized")
	}
	// A snapshot provides a consistent view of the DB at a point in time,
	// and its sequence number is the latest sequence number at that time.
	snap := wrs.db.NewSnapshot()
	defer snap.Close()
	return snap.SeqNum(), nil
}

// GetUpdatesSince scans the WAL files and returns all mutation events since a given sequence number.
func (wrs *WALReplicationServer) GetUpdatesSince(sinceSeq uint64) ([]WALUpdate, error) {
	if wrs.db == nil {
		return nil, fmt.Errorf("pebble db is not initialized")
	}

	walFiles, err := filepath.Glob(filepath.Join(wrs.config.DataDir, "*.log"))
	if err != nil {
		return nil, fmt.Errorf("failed to list WAL files: %w", err)
	}
	sort.Strings(walFiles)

	var updates []WALUpdate

	for _, walFile := range walFiles {
		f, err := os.Open(walFile)
		if err != nil {
			wrs.logger.Printf("WARNING: could not open WAL file %s: %v", walFile, err)
			continue
		}

		r := wal.NewReader(f, 0)
		for {
			rr, err := r.NextRecord()
			if err != nil {
				if err == io.EOF {
					break
				}
				// It's possible to hit partial records at the end of a file.
				// We can log this but continue to the next file.
				wrs.logger.Printf("WARNING: error reading next record from %s: %v", walFile, err)
				break
			}

			batchRepr, err := io.ReadAll(rr)
			if err != nil {
				wrs.logger.Printf("WARNING: could not read WAL record from %s: %v", walFile, err)
				continue
			}

			if len(batchRepr) < 12 { // Header size
				continue
			}

			// The batch representation starts with an 8-byte sequence number and 4-byte count.
			baseSeqNum := binary.LittleEndian.Uint64(batchRepr[:8])
			count := binary.LittleEndian.Uint32(batchRepr[8:12])

			// If the entire batch is before the sequence number we're interested in, skip it.
			if baseSeqNum+uint64(count) <= sinceSeq {
				continue
			}

			var batch pebble.Batch
			if err := batch.SetRepr(batchRepr); err != nil {
				wrs.logger.Printf("WARNING: could not decode batch from WAL file %s: %v", walFile, err)
				continue
			}

			iter, err := batch.NewIter(nil)
			if err != nil {
				wrs.logger.Printf("WARNING: could not create iterator for batch from WAL file %s: %v", walFile, err)
				continue
			}

			currentSeqNum := baseSeqNum
			for iter.First(); iter.Valid(); iter.Next() {
				if currentSeqNum >= sinceSeq {
					var op string
					var value string

					switch iter.Kind() {
					case pebble.InternalKeyKindSet:
						op = "put"
						value = string(iter.Value())
					case pebble.InternalKeyKindDelete:
						op = "delete"
					default:
						// Skip other kinds of operations (e.g., Merge, LogData)
						currentSeqNum++
						continue
					}

					updates = append(updates, WALUpdate{
						SeqNum: currentSeqNum,
						Op:     op,
						Key:    string(iter.Key()),
						Value:  value,
					})
				}
				currentSeqNum++
			}
			iter.Close()
		}
		f.Close() // Close file at the end of processing it.
	}
	return updates, nil
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

// handleWALUpdates is the HTTP handler for /wal/updates requests.
func (wrs *WALReplicationServer) handleWALUpdates(w http.ResponseWriter, r *http.Request) {
	sinceStr := r.URL.Query().Get("since")
	if sinceStr == "" {
		http.Error(w, "`since` parameter is required", http.StatusBadRequest)
		return
	}

	since, err := strconv.ParseUint(sinceStr, 10, 64)
	if err != nil {
		http.Error(w, "invalid `since` parameter, must be a non-negative integer", http.StatusBadRequest)
		return
	}

	updates, err := wrs.GetUpdatesSince(since)
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to get updates from WAL: %v", err), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(updates); err != nil {
		wrs.logger.Printf("ERROR: Failed to encode updates to JSON: %v", err)
		http.Error(w, "Failed to encode response", http.StatusInternalServerError)
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

// handleWALSeqNum is the HTTP handler for /wal/seqnum requests.
func (wrs *WALReplicationServer) handleWALSeqNum(w http.ResponseWriter, r *http.Request) {
	seqNum, err := wrs.GetLatestSequenceNumber()
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to get latest sequence number: %v", err), http.StatusInternalServerError)
		return
	}
	fmt.Fprint(w, strconv.FormatUint(seqNum, 10))
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
