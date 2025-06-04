package raft_replicator

import (
	"fmt"
	"log"
	"net/http"
	"strings"
)

func SetupHTTPServer(addr string, store *KVReplicator, logger *log.Logger) {
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
