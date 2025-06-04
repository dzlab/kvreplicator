package main

import (
	"fmt"
	"kvreplicator/raft_replicator"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/spf13/cobra"
)

var (
	nodeID           string
	raftBindAddr     string
	raftDataDir      string
	dbDataDir        string
	bootstrapCluster bool
	joinAddrStr      string
	httpAddr         string
)

var rootCmd = &cobra.Command{
	Use:   "kvreplicator-server",
	Short: "A distributed key-value store server using Raft",
	Long:  `kvreplicator-server is a server application that provides a distributed key-value store using the Raft consensus algorithm for replication.`,
}

var raftCmd = &cobra.Command{
	Use:   "raft",
	Short: "Run the KVReplicator Raft server",
	Long:  `Starts a KVReplicator server node, participating in a Raft cluster.`,
	Run: func(cmd *cobra.Command, args []string) {
		if nodeID == "" {
			log.Fatal("-id is required")
		}
		if raftBindAddr == "" {
			log.Fatal("-raftaddr is required")
		}
		if raftDataDir == "" {
			log.Fatal("-raftdir is required")
		}
		// rocksDBDataDir could be optional if the FSM defaults it, but good to specify
		if dbDataDir == "" {
			log.Printf("WARN: -dbdir is not set, using default path logic if any within FSM")
		}

		// Ensure data directories exist
		if err := os.MkdirAll(raftDataDir, 0700); err != nil {
			log.Fatalf("Failed to create Raft data directory: %v", err)
		}
		if err := os.MkdirAll(dbDataDir, 0700); err != nil {
			log.Fatalf("Failed to create DB data directory: %v", err)
		}

		logger := log.New(os.Stdout, fmt.Sprintf("[%s] ", nodeID), log.LstdFlags|log.Lmicroseconds)

		var joinAddresses []string
		if joinAddrStr != "" {
			joinAddresses = strings.Split(joinAddrStr, ",")
		}
		config := raft_replicator.DefaultConfig(nodeID, raftBindAddr, raftDataDir, dbDataDir)
		config.WithClusterConfig(bootstrapCluster, joinAddresses)

		kvStore, err := raft_replicator.NewKVReplicator(config)
		if err != nil {
			logger.Fatalf("Failed to create KVReplicator: %v", err)
		}

		if err := kvStore.Start(); err != nil {
			logger.Fatalf("Failed to start KVReplicator: %v", err)
		}

		logger.Printf("KVReplicator node %s started successfully.", nodeID)
		logger.Printf("Raft listening on: %s", raftBindAddr)
		logger.Printf("HTTP API listening on: %s", httpAddr)

		// Setup HTTP server for API
		setupHTTPServer(httpAddr, kvStore, logger)

		// Wait for termination signal
		quit := make(chan os.Signal, 1)
		signal.Notify(quit, os.Interrupt, syscall.SIGTERM)
		sig := <-quit
		logger.Printf("Received signal: %s. Shutting down...", sig)

		if err := kvStore.Shutdown(); err != nil {
			logger.Fatalf("Error during shutdown: %v", err)
		}
		logger.Println("KVReplicator shut down gracefully.")
	},
}

func init() {
	raftCmd.PersistentFlags().StringVar(&nodeID, "id", "node1", "Unique ID for this node")
	raftCmd.PersistentFlags().StringVar(&raftBindAddr, "raftaddr", "localhost:7000", "Raft bind address (host:port)")
	raftCmd.PersistentFlags().StringVar(&raftDataDir, "raftdir", "raft-data", "Raft data directory")
	raftCmd.PersistentFlags().StringVar(&dbDataDir, "dbdir", "db-data", "DB data directory")
	raftCmd.PersistentFlags().BoolVar(&bootstrapCluster, "bootstrap", false, "Bootstrap a new cluster (only for the first node)")
	raftCmd.PersistentFlags().StringVar(&joinAddrStr, "join", "", "Comma-separated addresses of cluster members to join (e.g., localhost:7000,localhost:7001)")
	raftCmd.PersistentFlags().StringVar(&httpAddr, "httpaddr", "localhost:8080", "HTTP API server address (host:port)")

	rootCmd.AddCommand(raftCmd)
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintf(os.Stderr, "Error executing command: %v\n", err)
		os.Exit(1)
	}
}

func setupHTTPServer(addr string, store *raft_replicator.KVReplicator, logger *log.Logger) {
	mux := http.NewServeMux()

	mux.HandleFunc("/kv", func(w http.ResponseWriter, r *http.Request) {
		key := r.URL.Query().Get("key")
		if key == "" {
			http.Error(w, "key parameter is required", http.StatusBadRequest)
			return
		}

		switch r.Method {
		case http.MethodGet:
			value, err := store.Get(key)
			if err != nil {
				// Differentiate between not found and other errors
				if strings.Contains(err.Error(), "key not found") {
					http.Error(w, err.Error(), http.StatusNotFound)
				} else {
					http.Error(w, fmt.Sprintf("Failed to get key: %v", err), http.StatusInternalServerError)
				}
				return
			}
			fmt.Fprint(w, value)
		case http.MethodPut:
			value := r.URL.Query().Get("value")
			if value == "" {
				// Allow empty value for PUT? For now, require it.
				http.Error(w, "value parameter is required for PUT", http.StatusBadRequest)
				return
			}
			err := store.Put(key, value)
			if err != nil {
				// Check if it's a "not leader" error
				if strings.Contains(err.Error(), "not the leader") {
					leaderAddr := store.Leader()
					if leaderAddr != "" {
						// Basic redirect hint. In a real client, you'd handle this by retrying on the leader.
						// The URL here is a guess; client needs to know leader's HTTP port.
						// For simplicity, assume leader HTTP is on defaultPort + 1 relative to its raft port.
						// This is a very naive assumption.
						http.Error(w, fmt.Sprintf("Not leader. Try leader at (Raft Address): %s. Error: %v", leaderAddr, err), http.StatusConflict) // 409 Conflict
					} else {
						http.Error(w, fmt.Sprintf("Failed to put key (not leader, leader unknown): %v", err), http.StatusServiceUnavailable)
					}
				} else {
					http.Error(w, fmt.Sprintf("Failed to put key: %v", err), http.StatusInternalServerError)
				}
				return
			}
			fmt.Fprint(w, "OK")
		case http.MethodDelete:
			err := store.Delete(key)
			if err != nil {
				if strings.Contains(err.Error(), "not the leader") {
					leaderAddr := store.Leader()
					if leaderAddr != "" {
						http.Error(w, fmt.Sprintf("Not leader. Try leader at (Raft Address): %s. Error: %v", leaderAddr, err), http.StatusConflict)
					} else {
						http.Error(w, fmt.Sprintf("Failed to delete key (not leader, leader unknown): %v", err), http.StatusServiceUnavailable)
					}
				} else {
					http.Error(w, fmt.Sprintf("Failed to delete key: %v", err), http.StatusInternalServerError)
				}
				return
			}
			fmt.Fprint(w, "OK")
		default:
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		}
	})

	mux.HandleFunc("/raft/join", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "POST only", http.StatusMethodNotAllowed)
			return
		}
		nodeID := r.URL.Query().Get("nodeId")
		raftAddr := r.URL.Query().Get("raftAddr")
		if nodeID == "" || raftAddr == "" {
			http.Error(w, "nodeId and raftAddr parameters are required", http.StatusBadRequest)
			return
		}
		if err := store.AddVoter(nodeID, raftAddr); err != nil {
			http.Error(w, fmt.Sprintf("Failed to add voter: %v", err), http.StatusInternalServerError)
			return
		}
		fmt.Fprint(w, "OK")
	})

	mux.HandleFunc("/raft/remove", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "POST only", http.StatusMethodNotAllowed)
			return
		}
		nodeID := r.URL.Query().Get("nodeId")
		if nodeID == "" {
			http.Error(w, "nodeId parameter is required", http.StatusBadRequest)
			return
		}
		if err := store.RemoveServer(nodeID); err != nil {
			http.Error(w, fmt.Sprintf("Failed to remove server: %v", err), http.StatusInternalServerError)
			return
		}
		fmt.Fprint(w, "OK")
	})

	mux.HandleFunc("/raft/leader", func(w http.ResponseWriter, r *http.Request) {
		leader := store.Leader()
		fmt.Fprintf(w, "Current leader Raft address: %s\n", leader)
		fmt.Fprintf(w, "Is this node leader: %t\n", store.IsLeader())
	})

	mux.HandleFunc("/raft/stats", func(w http.ResponseWriter, r *http.Request) {
		stats := store.Stats()
		for k, v := range stats {
			fmt.Fprintf(w, "%s: %s\n", k, v)
		}
	})

	server := &http.Server{
		Addr:    addr,
		Handler: mux,
	}

	go func() {
		logger.Printf("HTTP server listening on %s", addr)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Fatalf("HTTP server ListenAndServe: %v", err)
		}
	}()
}
