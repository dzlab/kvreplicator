package kvreplicator

import (
	"archive/tar"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/hashicorp/go-uuid"
	"github.com/hashicorp/raft"
)

// generateLocalSnapshotID is a helper to create a unique snapshot ID.
// This replicates the logic from github.com/hashicorp/raft.GenerateSnapshotID
// if it's not found by the compiler for any reason.
func generateLocalSnapshotID() (string, error) {
	id, err := uuid.GenerateUUID()
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%d-%s", time.Now().UnixNano(), id), nil
}

// pebbleFSM implements the raft.FSM interface, applying commands to Pebble.
type pebbleFSM struct {
	mu     sync.Mutex
	db     *pebble.DB
	dbPath string // Path to the Pebble data directory
	logger *log.Logger
}

// newPebbleFSM creates a new FSM.
func newPebbleFSM(dbPath string, logger *log.Logger) (*pebbleFSM, error) {
	if logger == nil {
		logger = log.New(os.Stderr, "[pebbleFSM] ", log.LstdFlags|log.Lmicroseconds)
	}
	if dbPath == "" {
		return nil, fmt.Errorf("dbPath cannot be empty")
	}

	// Ensure the DB path exists
	if err := os.MkdirAll(dbPath, 0755); err != nil {
		return nil, fmt.Errorf("failed to create pebble db path %s: %w", dbPath, err)
	}

	opts := &pebble.Options{
		// Logger: pebbleLoggerAdapter(logger), // Pebble expects a specific logger interface
		// For now, use Pebble's default logger or nil.
		// Consider adding a proper adapter if detailed Pebble logs are needed through raft's logger.
	}

	db, err := pebble.Open(dbPath, opts)
	if err != nil {
		logger.Printf("ERROR: failed to open pebble db at %s: %v", dbPath, err)
		return nil, fmt.Errorf("failed to open pebble db: %w", err)
	}
	logger.Printf("Pebble DB opened successfully at %s", dbPath)

	return &pebbleFSM{
		db:     db,
		dbPath: dbPath,
		logger: logger,
	}, nil
}

// Apply applies a Raft log entry to the key-value store.
func (fsm *pebbleFSM) Apply(logEntry *raft.Log) interface{} {
	fsm.mu.Lock()
	defer fsm.mu.Unlock()

	cmd, err := DeserializeCommand(logEntry.Data)
	if err != nil {
		fsm.logger.Printf("ERROR: failed to deserialize command: %v", err)
		return fmt.Errorf("deserialize command: %w", err)
	}

	fsm.logger.Printf("Applying command: Op=%s, Key=%s", cmd.Op, cmd.Key)

	writeOpts := pebble.Sync // Ensure data is flushed to disk

	switch cmd.Op {
	case OpPut:
		err = fsm.db.Set([]byte(cmd.Key), []byte(cmd.Value), writeOpts)
		if err != nil {
			fsm.logger.Printf("ERROR: Pebble Set failed for Key=%s: %v", cmd.Key, err)
			return err
		}
		fsm.logger.Printf("PUT: Key=%s, Value=%s", cmd.Key, cmd.Value)
		return nil // Return nil on success
	case OpDelete:
		err = fsm.db.Delete([]byte(cmd.Key), writeOpts)
		if err != nil {
			// Pebble's Delete doesn't error if key not found, it's a successful deletion of nothing.
			fsm.logger.Printf("ERROR: Pebble Delete failed for Key=%s: %v", cmd.Key, err)
			return err
		}
		fsm.logger.Printf("DELETE: Key=%s", cmd.Key)
		return nil // Return nil on success
	default:
		err := fmt.Errorf("unrecognized command op: %s", cmd.Op)
		fsm.logger.Printf("ERROR: %v", err)
		return err
	}
}

// Snapshot returns a snapshot of the current state.
// For Pebble, this involves creating a Pebble checkpoint.
func (fsm *pebbleFSM) Snapshot() (raft.FSMSnapshot, error) {
	fsm.mu.Lock()
	defer fsm.mu.Unlock()

	fsm.logger.Println("Creating FSM snapshot (Pebble checkpoint)")

	// Create a temporary directory for the checkpoint.
	// Suffix with something unique or clean up carefully.
	// Raft snapshot IDs could be part of this. For now, a generic name.
	snapshotID, err := generateLocalSnapshotID()
	if err != nil {
		fsm.logger.Printf("ERROR: failed to generate snapshot ID: %v", err)
		return nil, fmt.Errorf("failed to generate snapshot ID: %w", err)
	}
	checkpointDir := filepath.Join(filepath.Dir(fsm.dbPath), fmt.Sprintf("%s_snapshot_tmp_%s", filepath.Base(fsm.dbPath), snapshotID))

	// Ensure old temp checkpoint dir is removed if it exists
	if err := os.RemoveAll(checkpointDir); err != nil {
		fsm.logger.Printf("WARN: failed to remove old temp checkpoint dir %s: %v", checkpointDir, err)
		// Continue, Checkpoint might fail if dir exists and is not empty
	}


	err = fsm.db.Checkpoint(checkpointDir)
	if err != nil {
		fsm.logger.Printf("ERROR: failed to create Pebble checkpoint at %s: %v", checkpointDir, err)
		return nil, fmt.Errorf("pebble checkpoint failed: %w", err)
	}

	fsm.logger.Printf("Pebble checkpoint created successfully at %s", checkpointDir)

	return &pebbleFSMSnapshot{
		checkpointDir: checkpointDir,
		logger:        fsm.logger,
	}, nil
}

// Restore restores the FSM to a previous state from a snapshot.
// For Pebble, this involves clearing the current DB and loading data from the checkpoint archive.
func (fsm *pebbleFSM) Restore(rc io.ReadCloser) error {
	fsm.mu.Lock()
	defer fsm.mu.Unlock()
	defer rc.Close()

	fsm.logger.Println("Restoring FSM from Pebble snapshot")

	// 1. Close the current Pebble DB
	if err := fsm.db.Close(); err != nil {
		fsm.logger.Printf("ERROR: failed to close Pebble DB before restore: %v", err)
		return fmt.Errorf("failed to close current db for restore: %w", err)
	}
	fsm.logger.Println("Current Pebble DB closed.")

	// 2. Clean existing DB directory
	fsm.logger.Printf("Removing existing Pebble DB directory: %s", fsm.dbPath)
	if err := os.RemoveAll(fsm.dbPath); err != nil {
		fsm.logger.Printf("ERROR: failed to remove existing Pebble DB directory %s: %v", fsm.dbPath, err)
		// Try to reopen the original DB if we can't proceed with restore.
		if _, openErr := newPebbleFSM(fsm.dbPath, fsm.logger); openErr != nil {
			fsm.logger.Printf("FATAL: Could not remove old DB dir and could not reopen DB %s: %v", fsm.dbPath, openErr)
		}
		return fmt.Errorf("failed to remove existing db directory %s: %w", fsm.dbPath, err)
	}

	// 3. Create new (empty) DB directory
	fsm.logger.Printf("Creating new Pebble DB directory: %s", fsm.dbPath)
	if err := os.MkdirAll(fsm.dbPath, 0755); err != nil {
		fsm.logger.Printf("ERROR: failed to create new Pebble DB directory %s: %v", fsm.dbPath, err)
		return fmt.Errorf("failed to create new db directory %s: %w", fsm.dbPath, err)
	}

	// 4. Read the archive from rc and extract its contents into fsm.dbPath
	fsm.logger.Printf("Extracting snapshot archive to %s", fsm.dbPath)
	tr := tar.NewReader(rc) // Assuming snapshot is a tarball
	for {
		header, err := tr.Next()
		if err == io.EOF {
			break // End of archive
		}
		if err != nil {
			fsm.logger.Printf("ERROR: failed to read tar header from snapshot: %v", err)
			return fmt.Errorf("failed to read tar header: %w", err)
		}

		target := filepath.Join(fsm.dbPath, header.Name)

		switch header.Typeflag {
		case tar.TypeDir:
			if _, err := os.Stat(target); err != nil {
				if err := os.MkdirAll(target, os.FileMode(header.Mode)); err != nil {
					fsm.logger.Printf("ERROR: failed to create directory from snapshot %s: %v", target, err)
					return fmt.Errorf("failed to MkdirAll %s: %w", target, err)
				}
			}
		case tar.TypeReg:
			outFile, err := os.OpenFile(target, os.O_CREATE|os.O_RDWR, os.FileMode(header.Mode))
			if err != nil {
				fsm.logger.Printf("ERROR: failed to create file from snapshot %s: %v", target, err)
				return fmt.Errorf("failed to create file %s: %w", target, err)
			}
			if _, err := io.Copy(outFile, tr); err != nil {
				outFile.Close()
				fsm.logger.Printf("ERROR: failed to write file data from snapshot %s: %v", target, err)
				return fmt.Errorf("failed to copy data to %s: %w", target, err)
			}
			outFile.Close()
		default:
			fsm.logger.Printf("WARN: unsupported tar entry type %c for file %s in snapshot", header.Typeflag, header.Name)
		}
	}
	fsm.logger.Println("Snapshot archive extracted.")

	// 5. Re-open Pebble DB
	opts := &pebble.Options{} // Use same options as in newPebbleFSM
	var err error
	fsm.db, err = pebble.Open(fsm.dbPath, opts)
	if err != nil {
		fsm.logger.Printf("ERROR: failed to re-open Pebble DB at %s after restore: %v", fsm.dbPath, err)
		return fmt.Errorf("failed to open pebble db post-restore: %w", err)
	}

	fsm.logger.Println("FSM restored successfully from Pebble snapshot.")
	return nil
}

// pebbleFSMSnapshot implements raft.FSMSnapshot.
type pebbleFSMSnapshot struct {
	checkpointDir string // Path to the Pebble checkpoint directory
	logger        *log.Logger
}

// Persist saves the FSM snapshot. Raft calls this to write the snapshot to a sink.
// For Pebble, this means archiving the checkpoint directory.
func (s *pebbleFSMSnapshot) Persist(sink raft.SnapshotSink) error {
	s.logger.Printf("Persisting Pebble snapshot from checkpoint %s to sink ID: %s", s.checkpointDir, sink.ID())

	err := func() error {
		tw := tar.NewWriter(sink)
		defer tw.Close()

		// Walk the checkpoint directory and add files to the tar archive.
		return filepath.Walk(s.checkpointDir, func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return fmt.Errorf("failure accessing a path %s: %w", path, err)
			}

			// Get header for current file/dir
			header, err := tar.FileInfoHeader(info, info.Name()) // Use info.Name() for symlinks if any, but checkpoint shouldn't have complex ones.
			if err != nil {
				return fmt.Errorf("failed to get tar FileInfoHeader for %s: %w", path, err)
			}

			// Update header.Name to be relative to checkpointDir
			relPath, err := filepath.Rel(s.checkpointDir, path)
			if err != nil {
				return fmt.Errorf("failed to get relative path for %s: %w", path, err)
			}
			header.Name = relPath

			if err := tw.WriteHeader(header); err != nil {
				return fmt.Errorf("failed to write tar header for %s: %w", path, err)
			}

			// If not a directory, write file content
			if !info.IsDir() {
				file, err := os.Open(path)
				if err != nil {
					return fmt.Errorf("failed to open file %s for taring: %w", path, err)
				}
				defer file.Close()
				if _, err := io.Copy(tw, file); err != nil {
					return fmt.Errorf("failed to copy file %s content to tar archive: %w", path, err)
				}
			}
			return nil
		})
	}()

	if err != nil {
		s.logger.Printf("ERROR: failed during Pebble snapshot persist (tarring checkpoint %s): %v", s.checkpointDir, err)
		_ = sink.Cancel() // Attempt to cancel the sink on error
		// s.Release() // Clean up checkpointDir even on failed persist? Raft might retry. Release is separate.
		return err
	}

	// Close the sink to indicate completion.
	if err := sink.Close(); err != nil {
		s.logger.Printf("ERROR: failed to close snapshot sink: %v", err)
		// s.Release() might be called by Raft anyway.
		return err
	}

	s.logger.Printf("Pebble snapshot from checkpoint %s persisted successfully.", s.checkpointDir)
	return nil
}

// Release is called when Raft is finished with the snapshot.
// For Pebble, this means cleaning up the temporary checkpoint directory.
func (s *pebbleFSMSnapshot) Release() {
	s.logger.Printf("Releasing FSM snapshot (removing checkpoint directory: %s)", s.checkpointDir)
	if err := os.RemoveAll(s.checkpointDir); err != nil {
		s.logger.Printf("ERROR: failed to remove checkpoint directory %s during release: %v", s.checkpointDir, err)
	} else {
		s.logger.Printf("Checkpoint directory %s removed successfully.", s.checkpointDir)
	}
}

// Get retrieves a value for a given key directly from Pebble.
func (fsm *pebbleFSM) Get(key string) (string, error) {
	fsm.mu.Lock() // Although Pebble Get is thread-safe, this ensures consistency if other FSM ops modify state.
	defer fsm.mu.Unlock()

	valueBytes, closer, err := fsm.db.Get([]byte(key))
	if err != nil {
		if err == pebble.ErrNotFound {
			return "", fmt.Errorf("key not found: %s", key) // Specific error for not found
		}
		fsm.logger.Printf("ERROR: Pebble Get failed for key %s: %v", key, err)
		return "", fmt.Errorf("pebble get for key %s failed: %w", key, err)
	}
	defer closer.Close()

	// Make a copy of the valueBytes if it's unsafe to use after closer.Close()
	// Pebble's documentation implies the slice is valid until closer is closed.
	// To be safe, copy it.
	valueStr := string(valueBytes)

	return valueStr, nil
}

// Close closes the FSM and its underlying Pebble database.
func (fsm *pebbleFSM) Close() error {
	fsm.mu.Lock()
	defer fsm.mu.Unlock()

	if fsm.db != nil {
		err := fsm.db.Close()
		if err != nil {
			fsm.logger.Printf("ERROR: failed to close Pebble DB: %v", err)
			return err
		}
		fsm.logger.Println("Pebble DB closed.")
		fsm.db = nil
	} else {
		fsm.logger.Println("FSM Close called, but Pebble DB was already nil.")
	}
	return nil
}