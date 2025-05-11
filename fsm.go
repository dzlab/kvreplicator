```go
package kvreplicator

import (
	"fmt"
	"io"
	"log" // Or your preferred logging library
	"sync"

	"github.com/hashicorp/raft"
	// You would import your RocksDB Go wrapper here, e.g.:
	// "github.com/tecbot/gorocksdb"
)

// rocksDBFSM implements the raft.FSM interface, applying commands to RocksDB.
type rocksDBFSM struct {
	mu sync.Mutex
	// db *gorocksdb.DB // Placeholder for the actual RocksDB instance
	// dbPath string     // Path to the RocksDB data directory

	// For demonstration, we'll use a simple in-memory map.
	// In a real implementation, this would interact with RocksDB.
	data map[string]string
	logger *log.Logger
}

// newRocksDBFSM creates a new FSM.
// In a real scenario, 'dbPath' would be used to open/create the RocksDB instance.
func newRocksDBFSM(logger *log.Logger) *rocksDBFSM {
	// In a real implementation:
	// opts := gorocksdb.NewDefaultOptions()
	// opts.SetCreateIfMissing(true)
	// db, err := gorocksdb.OpenDb(opts, dbPath)
	// if err != nil {
	// 	 logger.Fatalf("failed to open rocksdb: %v", err)
	// }
	return &rocksDBFSM{
		data:   make(map[string]string),
		logger: logger,
		// db: db,
		// dbPath: dbPath,
	}
}

// Apply applies a Raft log entry to the key-value store.
func (fsm *rocksDBFSM) Apply(logEntry *raft.Log) interface{} {
	fsm.mu.Lock()
	defer fsm.mu.Unlock()

	cmd, err := DeserializeCommand(logEntry.Data)
	if err != nil {
		fsm.logger.Printf("ERROR: failed to deserialize command: %v", err)
		// Returning an error here can signal to Raft that the command failed.
		// Depending on the Raft configuration, this might lead to retries or other behavior.
		return fmt.Errorf("deserialize command: %w", err)
	}

	fsm.logger.Printf("Applying command: Op=%s, Key=%s", cmd.Op, cmd.Key)

	// In a real implementation, you would use RocksDB operations:
	// writeOpts := gorocksdb.NewDefaultWriteOptions()
	// defer writeOpts.Destroy()
	// readOpts := gorocksdb.NewDefaultReadOptions()
	// defer readOpts.Destroy()

	switch cmd.Op {
	case OpPut:
		// err = fsm.db.Put(writeOpts, []byte(cmd.Key), []byte(cmd.Value))
		// if err != nil {
		// 	 fsm.logger.Printf("ERROR: RocksDB Put failed: %v", err)
		// 	 return err
		// }
		fsm.data[cmd.Key] = cmd.Value
		fsm.logger.Printf("PUT: Key=%s, Value=%s", cmd.Key, cmd.Value)
		return nil // Return nil on success
	case OpDelete:
		// err = fsm.db.Delete(writeOpts, []byte(cmd.Key))
		// if err != nil {
		// 	 fsm.logger.Printf("ERROR: RocksDB Delete failed: %v", err)
		// 	 return err
		// }
		delete(fsm.data, cmd.Key)
		fsm.logger.Printf("DELETE: Key=%s", cmd.Key)
		return nil // Return nil on success
	default:
		err := fmt.Errorf("unrecognized command op: %s", cmd.Op)
		fsm.logger.Printf("ERROR: %v", err)
		return err
	}
}

// Snapshot returns a snapshot of the current state.
// For RocksDB, this would involve creating a RocksDB checkpoint or iterating over keys.
func (fsm *rocksDBFSM) Snapshot() (raft.FSMSnapshot, error) {
	fsm.mu.Lock()
	defer fsm.mu.Unlock()

	fsm.logger.Println("Creating FSM snapshot")

	// In a real RocksDB implementation, you'd create a checkpoint or iterate
	// through the DB and serialize it. For this example, we'll copy the in-memory map.
	// This is a simplified snapshot for demonstration.
	// A real RocksDB snapshot would be more complex to avoid holding the lock for too long.

	// Create a copy of the data to avoid race conditions
	dataCopy := make(map[string]string)
	for k, v := range fsm.data {
		dataCopy[k] = v
	}

	return &rocksDBFSMSnapshot{
		data:   dataCopy,
		logger: fsm.logger,
	}, nil
}

// Restore restores the FSM to a previous state.
// For RocksDB, this might involve clearing the current DB and loading data from the snapshot.
func (fsm *rocksDBFSM) Restore(rc io.ReadCloser) error {
	fsm.mu.Lock()
	defer fsm.mu.Unlock()

	fsm.logger.Println("Restoring FSM from snapshot")

	// In a real RocksDB implementation, you would deserialize the snapshot data
	// (e.g., from a RocksDB checkpoint backup or a serialized key-value stream)
	// and write it into the database. This often involves clearing existing data first.

	// For this example with an in-memory map:
	// We'll assume the snapshot is a serialized version of our Command struct,
	// representing a series of Puts to rebuild the state.
	// A more robust snapshot format would be used in a real system (e.g., JSON, gob of the map).

	// This is a highly simplified restore. A real RocksDB restore would be more involved.
	// For example, it might involve reading a backup file created by RocksDB's checkpoint mechanism.
	// Here, we'll just clear and repopulate the map based on a simple format read from rc.
	// We expect a series of Command objects serialized by rocksDBFSMSnapshot.Persist.

	newData := make(map[string]string)
	// This is where you would read the snapshot data from 'rc' and deserialize it.
	// For RocksDB, this might be a complex process involving checkpoint files or a custom format.
	// For our simplified in-memory example, let's assume the snapshot is just a JSON stream of commands.
	// However, the FSMSnapshot.Persist method below uses a custom, simpler format.

	// Let's assume the snapshot is just a series of "key=value" lines for simplicity.
	// A real implementation would use a more robust serialization format (e.g., gob, protobuf, or json stream).
	// For now, we'll just log and set the data to an empty map, as the snapshot persistence
	// is also simplified. A proper implementation would deserialize the data written by Persist.

	// Clear current data
	fsm.data = make(map[string]string)

	// This is a placeholder. A real implementation would read data from 'rc'.
	// For example, if rocksDBFSMSnapshot.Persist wrote JSON:
	/*
	   decoder := json.NewDecoder(rc)
	   if err := decoder.Decode(&fsm.data); err != nil {
	       fsm.logger.Printf("ERROR: failed to decode snapshot data: %v", err)
	       return fmt.Errorf("failed to decode snapshot: %w", err)
	   }
	*/
	// Since our rocksDBFSMSnapshot.Persist is simplified and doesn't write a full map,
	// this Restore implementation is also simplified.
	// In a real scenario, Persist and Restore must be compatible.

	fsm.logger.Println("FSM restored. (Simplified: data cleared, real restore needed)")
	// If using RocksDB, you'd load data from a checkpoint or iterate through the snapshot source.
	return nil
}

// rocksDBFSMSnapshot implements raft.FSMSnapshot.
type rocksDBFSMSnapshot struct {
	// In a real RocksDB snapshot, this might be a path to a checkpoint,
	// or an iterator. For our in-memory example, it's a copy of the data.
	data   map[string]string
	logger *log.Logger
}

// Persist saves the FSM snapshot. Raft calls this to write the snapshot to a sink.
func (s *rocksDBFSMSnapshot) Persist(sink raft.SnapshotSink) error {
	s.logger.Printf("Persisting snapshot to sink ID: %s", sink.ID())

	// In a real RocksDB implementation, you would stream the data from your
	// checkpoint or database iterator to the sink.
	// For example, you might iterate over all key-value pairs and write them.

	// For our simplified in-memory example, we'll serialize the map.
	// A more efficient format like gob or protobuf would be better for production.
	// We are writing key=value pairs, one per line. This is very basic.
	// A production system would use a more robust method (e.g., gob encoding the map, or JSON).
	err := func() error {
		// Example: Write data as JSON. A real system might use a more compact binary format.
		// Or, if using RocksDB checkpoints, you might copy checkpoint files to the sink.
		// For this example, let's imagine writing commands that would recreate the state.
		for k, v := range s.data {
			cmd := Command{Op: OpPut, Key: k, Value: v}
			serializedCmd, err := cmd.Serialize()
			if err != nil {
				return fmt.Errorf("failed to serialize command for snapshot: %w", err)
			}
			// Write length of command, then command itself
			// This is a simple framing mechanism.
			// lenBytes := make([]byte, 4) // Assuming length fits in uint32
			// binary.BigEndian.PutUint32(lenBytes, uint32(len(serializedCmd)))
			// if _, err := sink.Write(lenBytes); err != nil {
			// 	return fmt.Errorf("failed to write cmd length to sink: %w", err)
			// }
			if _, err := sink.Write(serializedCmd); err != nil {
				return fmt.Errorf("failed to write cmd data to sink: %w", err)
			}
			// Add a newline or some delimiter if necessary for your Restore logic
			if _, err := sink.Write([]byte("\n")); err != nil {
				return fmt.Errorf("failed to write newline to sink: %w", err)
			}

		}
		return nil
	}()

	if err != nil {
		s.logger.Printf("ERROR: failed during snapshot persist: %v", err)
		_ = sink.Cancel() // Attempt to cancel the sink on error
		return err
	}

	// Close the sink to indicate completion.
	if err := sink.Close(); err != nil {
		s.logger.Printf("ERROR: failed to close snapshot sink: %v", err)
		return err
	}
	s.logger.Println("Snapshot persisted successfully.")
	return nil
}

// Release is called when Raft is finished with the snapshot.
func (s *rocksDBFSMSnapshot) Release() {
	s.logger.Println("Releasing FSM snapshot")
	// If you had allocated resources for the snapshot (e.g., opened files, DB iterators),
	// you would release them here. For our in-memory example, there's nothing to do.
	s.data = nil // Allow GC
}

// Get is a helper method to read a value from the FSM's data.
// This is not part of the raft.FSM interface but useful for the application.
// In a real system, this would read directly from RocksDB.
func (fsm *rocksDBFSM) Get(key string) (string, bool) {
	fsm.mu.Lock()
	defer fsm.mu.Unlock()

	// In a real implementation:
	// readOpts := gorocksdb.NewDefaultReadOptions()
	// defer readOpts.Destroy()
	// val, err := fsm.db.Get(readOpts, []byte(key))
	// if err != nil {
	//    fsm.logger.Printf("ERROR: RocksDB Get failed for key %s: %v", key, err)
	//    return "", false
	// }
	// if val.Data() == nil { // Key not found
	//    return "", false
	// }
	// valueStr := string(val.Data())
	// val.Free() // Important to free the C-allocated memory
	// return valueStr, true

	// Using in-memory map for example:
	val, ok := fsm.data[key]
	return val, ok
}

// Close closes the FSM and its underlying database.
func (fsm *rocksDBFSM) Close() error {
	fsm.mu.Lock()
	defer fsm.mu.Unlock()

	// if fsm.db != nil {
	// 	fsm.db.Close()
	// }
	fsm.logger.Println("FSM closed.")
	return nil
}
```