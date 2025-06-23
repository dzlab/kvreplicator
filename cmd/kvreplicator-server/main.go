package main

import (
	"fmt"
	"kvreplicator/raft_replicator"
	"kvreplicator/wal_replicator"
	"log"
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

// Variables specifically for the WAL subcommand
var (
	walNodeID    string
	walBindAddr  string // Internal bind address for WAL replication communication
	walDataDir   string // Directory for WAL files, DB, etc.
	walHTTPAddr  string // HTTP API server address for WAL
	walZkServers string // Comma-separated addresses of Zookeeper servers
)

var rootCmd = &cobra.Command{
	Use:   "kvreplicator-server",
	Short: "A distributed key-value store server using Raft or WAL replication",
	Long:  `kvreplicator-server is a server application that provides a distributed key-value store using either the Raft consensus algorithm or WAL replication.`,
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

		server, err := raft_replicator.NewRaftReplicationServer(config)
		if err != nil {
			logger.Fatalf("Failed to create RAFT Replication Server: %v", err)
		}

		if err := server.Start(httpAddr); err != nil {
			logger.Fatalf("Failed to start RAFT Replication Server: %v", err)
		}

		// Wait for termination signal
		quit := make(chan os.Signal, 1)
		signal.Notify(quit, os.Interrupt, syscall.SIGTERM)
		sig := <-quit
		logger.Printf("Received signal: %s. Shutting down...", sig)

		if err := server.Shutdown(); err != nil {
			logger.Fatalf("Error during shutdown: %v", err)
		}
		logger.Println("KVReplicator shut down gracefully.")
	},
}

var walCmd = &cobra.Command{
	Use:   "wal",
	Short: "Run the KVReplicator WAL server (placeholder)",
	Long:  `Starts a KVReplicator server node using WAL replication (placeholder implementation).`,
	Run: func(cmd *cobra.Command, args []string) {
		if walNodeID == "" {
			log.Fatal("-id is required")
		}
		if walBindAddr == "" {
			log.Fatal("-bindaddr is required")
		}
		if walDataDir == "" {
			log.Fatal("-datadir is required")
		}

		// Ensure data directory exists
		if err := os.MkdirAll(walDataDir, 0700); err != nil {
			log.Fatalf("Failed to create WAL data directory: %v", err)
		}

		logger := log.New(os.Stdout, fmt.Sprintf("[%s-wal] ", walNodeID), log.LstdFlags|log.Lmicroseconds)
		logger.Println("Starting WAL replicator server (placeholder)...")

		var zkServers []string
		if walZkServers != "" {
			zkServers = strings.Split(walZkServers, ",")
		}

		// Placeholder WALConfig and Server creation
		cfg := wal_replicator.WALConfig{
			NodeID:              walNodeID,
			InternalBindAddress: walBindAddr,
			HTTPAddr:            walHTTPAddr,
			DataDir:             walDataDir,
			ZkServers:           zkServers, // Include Zookeeper servers in config
		}

		server, err := wal_replicator.NewWALReplicationServer(cfg)
		if err != nil {
			logger.Fatalf("Failed to create WAL Replication Server (placeholder): %v", err)
		}

		if err := server.Start(); err != nil {
			logger.Fatalf("Failed to start WAL Replication Server (placeholder): %v", err)
		}

		// Wait for termination signal
		quit := make(chan os.Signal, 1)
		signal.Notify(quit, os.Interrupt, syscall.SIGTERM)
		sig := <-quit
		logger.Printf("Received signal: %s. Shutting down...", sig)

		if err := server.Shutdown(); err != nil {
			logger.Fatalf("Error during placeholder WAL server shutdown: %v", err)
		}
		logger.Println("WAL replicator placeholder shut down gracefully.")
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

	walCmd.PersistentFlags().StringVar(&walNodeID, "id", "wal-node1", "Unique ID for this node")
	walCmd.PersistentFlags().StringVar(&walBindAddr, "bindaddr", "localhost:7000", "Internal bind address for replication (host:port)")
	walCmd.PersistentFlags().StringVar(&walDataDir, "datadir", "wal-data", "Data directory for WAL files and DB")
	walCmd.PersistentFlags().StringVar(&walHTTPAddr, "httpaddr", "localhost:8081", "HTTP API server address (host:port)") // Default to a different port than Raft
	walCmd.PersistentFlags().StringVar(&walZkServers, "zkservers", "", "Comma-separated addresses of Zookeeper servers (e.g., localhost:2181)")

	rootCmd.AddCommand(raftCmd)
	rootCmd.AddCommand(walCmd)
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintf(os.Stderr, "Error executing command: %v\n", err)
		os.Exit(1)
	}
}
