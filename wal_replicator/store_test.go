package wal_replicator

import (
	"bytes"
	"fmt"
	"log"
	"os"
	"path/filepath" // Required for testing GetLatestSequenceNumber's reflection logic
	"testing"

	// Assuming this exists and provides DecodeInternalKey

	"github.com/cockroachdb/pebble"
	"github.com/stretchr/testify/assert"
)

// setupTestDB creates a temporary directory for PebbleDB and returns its path and a logger.
func setupTestDB(t *testing.T) (string, *log.Logger) {
	tempDir, err := os.MkdirTemp("", "pebbledb-test-")
	assert.NoError(t, err)

	var logBuffer bytes.Buffer
	logger := log.New(&logBuffer, "TEST_LOG: ", log.LstdFlags|log.Lshortfile)
	t.Logf("Test DB will be at: %s", tempDir)
	return tempDir, logger
}

// teardownTestDB closes the store and removes the temporary directory.
func teardownTestDB(t *testing.T, store *PebbleDBStore, dataDir string) {
	if store != nil {
		err := store.Close()
		assert.NoError(t, err)
	}
	if dataDir != "" {
		err := os.RemoveAll(dataDir)
		assert.NoError(t, err)
	}
}

func TestNewPebbleDBStore(t *testing.T) {
	// Test case 1: Successful initialization
	dataDir, logger := setupTestDB(t)
	defer teardownTestDB(t, nil, dataDir)

	store, err := NewPebbleDBStore(dataDir, logger)
	assert.NoError(t, err)
	assert.NotNil(t, store)
	assert.NotNil(t, store.db)
	assert.Equal(t, dataDir, store.dataDir)
	teardownTestDB(t, store, "") // Close the store explicitly before final teardown

	// Test case 2: Empty data directory
	store, err = NewPebbleDBStore("", logger)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "data directory must be specified")
	assert.Nil(t, store)

	// Test case 3: Failed to create data directory (simulate by creating a file with the same name)
	fileAsDataDir := filepath.Join(dataDir, "not-a-dir")
	err = os.WriteFile(fileAsDataDir, []byte("dummy"), 0644)
	assert.NoError(t, err)

	store, err = NewPebbleDBStore(fileAsDataDir, logger)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to create data directory for PebbleDB")
	assert.Nil(t, store)
	os.Remove(fileAsDataDir) // Clean up the dummy file

	// Test case 4: Failed to open Pebble DB (simulated by permission error for the data directory)
	readOnlyDir := filepath.Join(dataDir, "readonly-db-dir")
	err = os.Mkdir(readOnlyDir, 0755) // Create dir initially with write permissions
	assert.NoError(t, err)

	// Make the directory read-only for the user/group that will open it
	err = os.Chmod(readOnlyDir, 0444) // Read-only for all
	assert.NoError(t, err)

	store, err = NewPebbleDBStore(readOnlyDir, logger)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to open pebble db") // This should now be the error from pebble.Open
	assert.Nil(t, store)

	// Change permissions back to allow cleanup
	err = os.Chmod(readOnlyDir, 0755)
	assert.NoError(t, err)
	os.RemoveAll(readOnlyDir) // Clean up

	// Test case 5: Check if directory is created if it doesn't exist
	newDir := filepath.Join(dataDir, "new-test-dir")
	store, err = NewPebbleDBStore(newDir, logger)
	assert.NoError(t, err)
	assert.NotNil(t, store)
	assert.DirExists(t, newDir)
	teardownTestDB(t, store, newDir) // Clean up the newly created directory
}

func TestPebbleDBStoreClose(t *testing.T) {
	dataDir, logger := setupTestDB(t)
	defer teardownTestDB(t, nil, dataDir)

	store, err := NewPebbleDBStore(dataDir, logger)
	assert.NoError(t, err)
	assert.NotNil(t, store)

	// Test case 1: Successful close
	err = store.Close()
	assert.NoError(t, err)
	assert.Nil(t, store.db) // db should be set to nil after close

	// Test case 2: Closing an already closed DB (should be a no-op or return nil)
	err = store.Close()
	assert.NoError(t, err) // Should not error, as db is already nil
}

func TestPebbleDBStoreOperations(t *testing.T) {
	dataDir, logger := setupTestDB(t)
	store, err := NewPebbleDBStore(dataDir, logger)
	assert.NoError(t, err)
	assert.NotNil(t, store)
	defer teardownTestDB(t, store, dataDir)

	// Test case 1: Put and Get successful
	key1, value1 := "testKey1", "testValue1"
	err = store.Put(key1, value1)
	assert.NoError(t, err)

	retrievedValue, err := store.Get(key1)
	assert.NoError(t, err)
	assert.Equal(t, value1, retrievedValue)

	// Test case 2: Update value
	value1Updated := "updatedValue1"
	err = store.Put(key1, value1Updated)
	assert.NoError(t, err)
	retrievedValue, err = store.Get(key1)
	assert.NoError(t, err)
	assert.Equal(t, value1Updated, retrievedValue)

	// Test case 3: Get non-existent key
	_, err = store.Get("nonExistentKey")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "key not found")

	// Test case 4: Delete successful
	key2, value2 := "testKey2", "testValue2"
	err = store.Put(key2, value2)
	assert.NoError(t, err)
	retrievedValue, err = store.Get(key2)
	assert.NoError(t, err)
	assert.Equal(t, value2, retrievedValue)

	err = store.Delete(key2)
	assert.NoError(t, err)

	_, err = store.Get(key2) // Should now be not found
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "key not found")

	// Test case 5: Operations on a closed DB
	err = store.Close()
	assert.NoError(t, err)

	_, err = store.Get("anyKey")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "pebble db is not initialized")

	err = store.Put("anyKey", "anyValue")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "pebble db is not initialized")

	err = store.Delete("anyKey")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "pebble db is not initialized")
}

func TestPebbleDBStoreGetLatestSequenceNumber(t *testing.T) {
	dataDir, logger := setupTestDB(t)
	store, err := NewPebbleDBStore(dataDir, logger)
	assert.NoError(t, err)
	assert.NotNil(t, store)
	defer teardownTestDB(t, store, dataDir)

	// Test case 1: Initial sequence number
	initialSeqNum, err := store.GetLatestSequenceNumber()
	assert.NoError(t, err)
	// A new DB usually starts at a low sequence number (e.g., 0 or higher due to internal writes).
	// We just ensure it's not an error and capture the baseline for relative checks.
	t.Logf("Initial sequence number: %d", initialSeqNum)

	// Test case 2: Sequence number after puts
	err = store.Put("keyA", "valueA")
	assert.NoError(t, err) // 1st user operation
	currentSeqNum, err := store.GetLatestSequenceNumber()
	assert.NoError(t, err)
	assert.GreaterOrEqual(t, currentSeqNum, initialSeqNum+1, "Sequence number should increase by at least 1 after first Put")
	t.Logf("Sequence number after 1st put: %d", currentSeqNum)

	err = store.Put("keyB", "valueB")
	assert.NoError(t, err) // 2nd user operation
	currentSeqNumAfterTwoPuts, err := store.GetLatestSequenceNumber()
	assert.NoError(t, err)
	assert.GreaterOrEqual(t, currentSeqNumAfterTwoPuts, initialSeqNum+2, "Sequence number should increase by at least 2 after two Puts")
	t.Logf("Sequence number after 2nd put: %d", currentSeqNumAfterTwoPuts)

	// Test case 3: Sequence number after delete
	err = store.Delete("keyA")
	assert.NoError(t, err) // 3rd user operation
	currentSeqNumAfterDelete, err := store.GetLatestSequenceNumber()
	assert.NoError(t, err)
	assert.GreaterOrEqual(t, currentSeqNumAfterDelete, initialSeqNum+3, "Sequence number should increase by at least 3 after 2 Puts and 1 Delete")
	t.Logf("Sequence number after delete: %d", currentSeqNumAfterDelete)

	// Test case 4: On a closed DB
	err = store.Close()
	assert.NoError(t, err)
	_, err = store.GetLatestSequenceNumber()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "pebble db is not initialized")
}

// Helper function to set up a store with predefined WAL updates for common scenarios.
// The returned store is open and ready for operations.
// The caller is responsible for calling teardownTestDB(t, store, "") to close the store
// if the store is not nil.
func setupStoreWithWALUpdates(t *testing.T, dataDir string, logger *log.Logger) (*PebbleDBStore, []WALUpdate) {
	store, err := NewPebbleDBStore(dataDir, logger)
	assert.NoError(t, err)
	assert.NotNil(t, store)

	err = store.Put("k1", "v1")
	assert.NoError(t, err) // SeqNum 1
	err = store.Put("k2", "v2")
	assert.NoError(t, err) // SeqNum 2
	err = store.Delete("k1")
	assert.NoError(t, err) // SeqNum 3
	err = store.Put("k3", "v3")
	assert.NoError(t, err) // SeqNum 4

	// Close the DB to ensure WALs are written to disk.
	// Then re-open to ensure we read from disk, not memory.
	err = store.Close()
	assert.NoError(t, err)

	store, err = NewPebbleDBStore(dataDir, logger)
	assert.NoError(t, err)
	assert.NotNil(t, store)

	expectedUpdates := []WALUpdate{
		{SeqNum: 1, Op: "put", Key: "k1", Value: "v1"},
		{SeqNum: 2, Op: "put", Key: "k2", Value: "v2"},
		{SeqNum: 3, Op: "delete", Key: "k1", Value: ""},
		{SeqNum: 4, Op: "put", Key: "k3", Value: "v3"},
	}
	return store, expectedUpdates
}

func TestPebbleDBStoreGetUpdatesSince_NoUpdatesInitially(t *testing.T) {
	dataDir, logger := setupTestDB(t)
	defer teardownTestDB(t, nil, dataDir)

	store, err := NewPebbleDBStore(dataDir, logger)
	assert.NoError(t, err)
	assert.NotNil(t, store)
	defer teardownTestDB(t, store, "")

	updates, err := store.GetUpdatesSince(0)
	assert.NoError(t, err)
	assert.Empty(t, updates)
}

func TestPebbleDBStoreGetUpdatesSince_GetAllUpdates(t *testing.T) {
	dataDir, logger := setupTestDB(t)
	defer teardownTestDB(t, nil, dataDir)

	store, expectedUpdates := setupStoreWithWALUpdates(t, dataDir, logger)
	defer teardownTestDB(t, store, "")

	updates, err := store.GetUpdatesSince(0)
	assert.NoError(t, err)
	assert.Len(t, updates, 4)
	assert.Equal(t, expectedUpdates, updates)
}

func TestPebbleDBStoreGetUpdatesSince_SpecificSequence(t *testing.T) {
	dataDir, logger := setupTestDB(t)
	defer teardownTestDB(t, nil, dataDir)

	store, expectedUpdates := setupStoreWithWALUpdates(t, dataDir, logger)
	defer teardownTestDB(t, store, "")

	updates, err := store.GetUpdatesSince(2)
	assert.NoError(t, err)
	assert.Len(t, updates, 3)
	assert.Equal(t, expectedUpdates[1:], updates)
}

func TestPebbleDBStoreGetUpdatesSince_LatestSequence(t *testing.T) {
	dataDir, logger := setupTestDB(t)
	defer teardownTestDB(t, nil, dataDir)

	store, _ := setupStoreWithWALUpdates(t, dataDir, logger)
	defer teardownTestDB(t, store, "")

	latestSeq, err := store.GetLatestSequenceNumber()
	assert.NoError(t, err)
	updates, err := store.GetUpdatesSince(latestSeq)
	assert.NoError(t, err)
	assert.Empty(t, updates)
}

func TestPebbleDBStoreGetUpdatesSince_ClosedDB(t *testing.T) {
	dataDir, logger := setupTestDB(t)
	store, err := NewPebbleDBStore(dataDir, logger)
	assert.NoError(t, err)
	assert.NotNil(t, store)
	defer teardownTestDB(t, store, dataDir)

	err = store.Close()
	assert.NoError(t, err)

	_, err = store.GetUpdatesSince(0)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "pebble db is not initialized")
}

func TestPebbleDBStoreGetUpdatesSince_MissingWALFile(t *testing.T) {
	dataDir, logger := setupTestDB(t)
	defer teardownTestDB(t, nil, dataDir)

	// Generate enough logs to potentially get multiple WAL files
	storeToGenerateLogs, err := NewPebbleDBStore(dataDir, logger)
	assert.NoError(t, err)
	for i := 0; i < 100; i++ { // Perform enough operations to potentially roll over WAL files
		err = storeToGenerateLogs.Put(fmt.Sprintf("large_key_%d", i), fmt.Sprintf("large_value_%d", i))
		assert.NoError(t, err)
	}
	err = storeToGenerateLogs.Close()
	assert.NoError(t, err)

	walFiles, err := filepath.Glob(filepath.Join(dataDir, "*.log"))
	assert.NoError(t, err)
	assert.True(t, len(walFiles) >= 1)

	// Capture log output for warnings
	var logBuffer bytes.Buffer
	captureLogger := log.New(&logBuffer, "TEST_LOG: ", log.LstdFlags|log.Lshortfile)
	store, err := NewPebbleDBStore(dataDir, captureLogger) // Reopen with capturing logger
	assert.NoError(t, err)
	defer teardownTestDB(t, store, "")

	fileToRemove := walFiles[0] // Pick the first log file to remove
	err = os.Remove(fileToRemove)
	assert.NoError(t, err)

	updates, err := store.GetUpdatesSince(0)
	assert.NoError(t, err) // Should not error, just log a warning for missing file
	assert.True(t, len(updates) > 0)
	assert.Contains(t, logBuffer.String(), fmt.Sprintf("WARNING: could not open WAL file %s", fileToRemove))
}

func TestPebbleDBStoreGetUpdatesSince_CorruptedWALFile(t *testing.T) {
	dataDir, logger := setupTestDB(t)
	defer teardownTestDB(t, nil, dataDir)

	store, err := NewPebbleDBStore(dataDir, logger)
	assert.NoError(t, err)
	assert.NotNil(t, store)
	err = store.Close() // Close to ensure it's not holding a lock when we write over its file
	assert.NoError(t, err)

	corruptedFile := filepath.Join(dataDir, "000000.log") // Common first log file name for PebbleDB
	err = os.WriteFile(corruptedFile, []byte("this is corrupted data that is too short"), 0644)
	assert.NoError(t, err)

	var logBuffer bytes.Buffer
	captureLogger := log.New(&logBuffer, "TEST_LOG: ", log.LstdFlags|log.Lshortfile)
	store, err = NewPebbleDBStore(dataDir, captureLogger)
	assert.NoError(t, err)
	defer teardownTestDB(t, store, "")

	updates, err := store.GetUpdatesSince(0)
	assert.NoError(t, err) // Should not error, just log warnings
	assert.Contains(t, logBuffer.String(), fmt.Sprintf("WARNING: error reading next record from %s", corruptedFile))
	assert.Contains(t, logBuffer.String(), fmt.Sprintf("WARNING: could not decode batch from WAL file %s", corruptedFile))
}

func TestPebbleDBStoreGetUpdatesSince_ShortCorruptedData(t *testing.T) {
	dataDir, logger := setupTestDB(t)
	defer teardownTestDB(t, nil, dataDir)

	store, err := NewPebbleDBStore(dataDir, logger)
	assert.NoError(t, err)
	assert.NotNil(t, store)
	err = store.Close()
	assert.NoError(t, err)

	corruptedFileShort := filepath.Join(dataDir, "000001.log") // Use a different dummy file
	err = os.WriteFile(corruptedFileShort, []byte("short"), 0644)
	assert.NoError(t, err)

	var logBuffer bytes.Buffer
	captureLogger := log.New(&logBuffer, "TEST_LOG: ", log.LstdFlags|log.Lshortfile)
	store, err = NewPebbleDBStore(dataDir, captureLogger)
	assert.NoError(t, err)
	defer teardownTestDB(t, store, "")

	updates, err := store.GetUpdatesSince(0)
	assert.NoError(t, err)
	assert.Empty(t, logBuffer.String())
	assert.NotNil(t, updates)
}

func TestPebbleDBStoreGetUpdatesSince_LogDataKind(t *testing.T) {
	dataDir, logger := setupTestDB(t)
	defer teardownTestDB(t, nil, dataDir)

	store, err := NewPebbleDBStore(dataDir, logger)
	assert.NoError(t, err)
	assert.NotNil(t, store)
	defer teardownTestDB(t, store, "")

	// Apply a batch that includes LogData entries
	batchForCoverage := store.db.NewBatch()
	batchForCoverage.Set([]byte("cov_key1"), []byte("cov_value1"), nil) // SeqNum 1
	batchForCoverage.LogData([]byte("logdata_payload_1"), nil)          // SeqNum 2 (skipped by GetUpdatesSince)
	batchForCoverage.Set([]byte("cov_key2"), []byte("cov_value2"), nil) // SeqNum 3
	batchForCoverage.Delete([]byte("cov_key1"), nil)                    // SeqNum 4
	batchForCoverage.LogData([]byte("logdata_payload_2"), nil)          // SeqNum 5 (skipped)

	err = store.db.Apply(batchForCoverage, pebble.Sync)
	assert.NoError(t, err)

	// Close and reopen to ensure WALs are flushed and readable from disk.
	err = store.Close()
	assert.NoError(t, err)
	store, err = NewPebbleDBStore(dataDir, logger)
	assert.NoError(t, err)
	defer store.Close() // Ensure this re-opened store is closed

	updates, err := store.GetUpdatesSince(0)
	assert.NoError(t, err)
	assert.Len(t, updates, 3) // Only 3 non-LogData updates expected

	// Verify the content and sequence numbers.
	// The sequence numbers start from 1 for a new DB.
	expectedUpdatesCoverage := []WALUpdate{
		{SeqNum: 1, Op: "put", Key: "cov_key1", Value: "cov_value1"},
		{SeqNum: 3, Op: "put", Key: "cov_key2", Value: "cov_value2"},
		{SeqNum: 4, Op: "delete", Key: "cov_key1", Value: ""},
	}
	assert.Equal(t, expectedUpdatesCoverage, updates)
}
