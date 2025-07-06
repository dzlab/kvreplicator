package wal_replicator

import (
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"reflect"
	"sort"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/record"
)

// WALUpdate represents a single mutation event (Put or Delete) in the WAL.
// SeqNum is the sequence number associated with this update.
type WALUpdate struct {
	SeqNum uint64
	Op     string // "put" or "delete"
	Key    string
	Value  string // Value is empty for "delete" operations
}

// PebbleDBStore manages the interactions with the PebbleDB instance.
type PebbleDBStore struct {
	db      *pebble.DB
	logger  *log.Logger
	dataDir string // Storing DataDir for WAL file access
}

// NewPebbleDBStore creates and initializes a new PebbleDBStore.
func NewPebbleDBStore(dataDir string, logger *log.Logger) (*PebbleDBStore, error) {
	logger.Printf("Initializing PebbleDBStore with data dir %s", dataDir)

	if dataDir == "" {
		return nil, fmt.Errorf("data directory must be specified for PebbleDBStore")
	}

	// Ensure data directory exists
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		logger.Printf("ERROR: Failed to create data directory %s: %v", dataDir, err)
		return nil, fmt.Errorf("failed to create data directory for PebbleDB: %w", err)
	}

	// Open PebbleDB
	opts := &pebble.Options{} // Use default options for now
	db, err := pebble.Open(dataDir, opts)
	if err != nil {
		logger.Printf("ERROR: Failed to open Pebble DB at %s: %v", dataDir, err)
		return nil, fmt.Errorf("failed to open pebble db: %w", err)
	}
	logger.Printf("Pebble DB opened successfully at %s", dataDir)

	return &PebbleDBStore{
		db:      db,
		logger:  logger,
		dataDir: dataDir,
	}, nil
}

// Close closes the underlying PebbleDB instance.
func (pbs *PebbleDBStore) Close() error {
	pbs.logger.Println("Closing Pebble DB...")
	if pbs.db != nil {
		if err := pbs.db.Close(); err != nil {
			pbs.logger.Printf("ERROR: Failed to close Pebble DB: %v", err)
			return fmt.Errorf("failed to close pebble db: %w", err)
		}
		pbs.logger.Println("Pebble DB closed successfully.")
		pbs.db = nil
	}
	return nil
}

// Get retrieves a value for a given key from PebbleDB.
func (pbs *PebbleDBStore) Get(key string) (string, error) {
	pbs.logger.Printf("DB Get: key=%s", key)
	if pbs.db == nil {
		return "", fmt.Errorf("pebble db is not initialized")
	}

	valueBytes, closer, err := pbs.db.Get([]byte(key))
	if err != nil {
		if err == pebble.ErrNotFound {
			pbs.logger.Printf("Key not found: %s", key)
			return "", fmt.Errorf("key not found: %s", key)
		}
		pbs.logger.Printf("ERROR: Pebble Get failed for key %s: %v", key, err)
		return "", fmt.Errorf("pebble get failed for key %s: %w", key, err)
	}
	defer closer.Close()

	valueStr := string(valueBytes)
	pbs.logger.Printf("DB Get successful: key=%s", key)
	return valueStr, nil
}

// Put sets a key-value pair in PebbleDB.
func (pbs *PebbleDBStore) Put(key, value string) error {
	pbs.logger.Printf("DB Put: key=%s, value=%s", key, value)
	if pbs.db == nil {
		return fmt.Errorf("pebble db is not initialized")
	}

	writeOpts := pebble.Sync
	err := pbs.db.Set([]byte(key), []byte(value), writeOpts)
	if err != nil {
		pbs.logger.Printf("ERROR: Pebble Set failed for Key=%s: %v", key, err)
		return fmt.Errorf("pebble set failed for key %s: %w", key, err)
	}
	pbs.logger.Printf("Successfully set key %s", key)
	return nil
}

// Delete removes a key-value pair from PebbleDB.
func (pbs *PebbleDBStore) Delete(key string) error {
	pbs.logger.Printf("DB Delete: key=%s", key)
	if pbs.db == nil {
		return fmt.Errorf("pebble db is not initialized")
	}

	writeOpts := pebble.Sync
	err := pbs.db.Delete([]byte(key), writeOpts)
	if err != nil {
		pbs.logger.Printf("ERROR: Pebble Delete failed for Key=%s: %v", key, err)
		return fmt.Errorf("pebble delete failed for key %s: %w", key, err)
	}
	pbs.logger.Printf("Successfully deleted key %s", key)
	return nil
}

// GetLatestSequenceNumber retrieves the latest sequence number from PebbleDB.
func (pbs *PebbleDBStore) GetLatestSequenceNumber() (uint64, error) {
	if pbs.db == nil {
		return 0, fmt.Errorf("pebble db is not initialized")
	}
	snap := pbs.db.NewSnapshot()
	defer snap.Close()

	// Get the reflect.Value of the struct
	v := reflect.ValueOf(snap).Elem()
	// Access the unexported (private) field by name
	f := v.FieldByName("seqNum")
	if !f.IsValid() {
		return 0, fmt.Errorf("no such field: seqNum")
	}
	// Read the value (note: Can only read, not set)
	seqNum := f.Uint()

	return seqNum, nil
}

// GetUpdatesSince retrieves WAL updates from PebbleDB starting from a given sequence number.
func (pbs *PebbleDBStore) GetUpdatesSince(sinceSeq uint64) ([]WALUpdate, error) {
	if pbs.db == nil {
		return nil, fmt.Errorf("pebble db is not initialized")
	}

	walFiles, err := filepath.Glob(filepath.Join(pbs.dataDir, "*.log"))
	if err != nil {
		return nil, fmt.Errorf("failed to list WAL files: %w", err)
	}
	sort.Strings(walFiles)

	var updates []WALUpdate

	for _, walFile := range walFiles {
		f, err := os.Open(walFile)
		if err != nil {
			pbs.logger.Printf("WARNING: could not open WAL file %s: %v", walFile, err)
			continue
		}

		r := record.NewReader(f, 0)
		for {
			rr, err := r.Next()
			if err != nil {
				if err == io.EOF {
					break
				}
				pbs.logger.Printf("WARNING: error reading next record from %s: %v", walFile, err)
				break
			}

			batchRepr, err := io.ReadAll(rr)
			if err != nil {
				pbs.logger.Printf("WARNING: could not read WAL record from %s: %v", walFile, err)
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
				pbs.logger.Printf("WARNING: could not decode batch from WAL file %s: %v", walFile, err)
				continue
			}

			iter, err := batch.NewIter(nil)
			if err != nil {
				pbs.logger.Printf("WARNING: could not create iterator for batch from WAL file %s: %v", walFile, err)
				continue
			}

			currentSeqNum := baseSeqNum
			for iter.First(); iter.Valid(); iter.Next() {
				if currentSeqNum >= sinceSeq {
					var op string
					var value string
					keyBytes := iter.Key()
					valueBytes := iter.Value()
					keyString := string(keyBytes)

					// NOTE: Without access to pebble's internal/base package,
					// reliably distinguishing 'set' from 'delete' for WAL updates
					// based solely on public APIs is challenging, especially if a
					// key is set to an empty value. This implementation makes a
					// best-effort guess: if there's a value, it's a 'put', otherwise 'delete'.
					// This is an oversimplification and may not be accurate for all cases.
					if len(valueBytes) > 0 {
						op = "put"
						value = string(valueBytes)
					} else {
						op = "delete"
					}

					updates = append(updates, WALUpdate{
						SeqNum: currentSeqNum,
						Op:     op,
						Key:    keyString,
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
