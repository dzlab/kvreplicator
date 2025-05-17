package kvreplicator

import (
	"fmt"
	"io"
	"log"

	"github.com/hashicorp/raft"
)

// kvStore wraps the PebbleDB FSM and provides methods for KV operations
// as well as implementing the raft.FSM interface.
type kvStore struct {
	fsm    *pebbleFSM
	logger *log.Logger
}

// newKVStore initializes and returns a new kvStore.
// It primarily sets up the underlying PebbleDB FSM.
func newKVStore(dbPath string, logger *log.Logger) (*kvStore, error) {
	fsm, err := newPebbleFSM(dbPath, logger)
	if err != nil {
		return nil, fmt.Errorf("failed to create Pebble FSM: %w", err)
	}

	store := &kvStore{
		fsm:    fsm,
		logger: logger,
	}

	return store, nil
}

// Get retrieves a value for a given key directly from the FSM.
// This method does not go through Raft.
func (ks *kvStore) Get(key string) (string, error) {
	// pebbleFSM.Get now returns (string, error), where error can be "key not found"
	return ks.fsm.Get(key)
}

// Snapshot returns a raft.FSMSnapshot suitable for saving state to disk.
func (ks *kvStore) Snapshot() (raft.FSMSnapshot, error) {
	ks.logger.Println("Creating FSM snapshot...")
	snapshot, err := ks.fsm.Snapshot() // pebbleFSM implements FSMSnapshot
	if err != nil {
		ks.logger.Printf("ERROR: failed to create FSM snapshot: %v", err)
		return nil, err
	}
	ks.logger.Println("FSM snapshot created.")
	return snapshot, nil
}

// Restore restores the FSM from a raft.FSMSnapshot.
func (ks *kvStore) Restore(r io.ReadCloser) error {
	ks.logger.Println("Restoring FSM from snapshot...")
	err := ks.fsm.Restore(r)
	if err != nil {
		ks.logger.Printf("ERROR: failed to restore FSM from snapshot: %v", err)
		return err
	}
	ks.logger.Println("FSM restored from snapshot.")
	return nil
}

// Close closes the underlying PebbleDB FSM.
func (ks *kvStore) Close() error {
	ks.logger.Println("Closing FSM (Pebble DB)...")
	if err := ks.fsm.Close(); err != nil {
		ks.logger.Printf("ERROR: failed to close FSM (Pebble DB): %v", err)
		return err
	}
	ks.logger.Println("FSM (Pebble DB) closed.")
	return nil
}
