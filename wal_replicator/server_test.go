package wal_replicator

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
)

// Helper function to create a temporary directory for PebbleDB
func newTempDir(t *testing.T) string {
	dir, err := ioutil.TempDir("", "pebble-test-")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	return dir
}

// Helper function to clean up a directory
func cleanupDir(dir string) {
	os.RemoveAll(dir)
}

// Helper function to create a test server instance
func newTestWALServer(t *testing.T, dataDir string) (*WALReplicationServer, error) {
	cfg := WALConfig{
		NodeID:              "test-node",   // Use a consistent node ID
		InternalBindAddress: "127.0.0.1:0", // Use :0 for a random available port
		HTTPAddr:            ":0",          // Use :0 to get a random available port
		DataDir:             dataDir,       // Use the dataDir parameter
		// ZkServers is intentionally omitted to test behavior without a ZK connection
	}
	return NewWALReplicationServer(cfg)
}

func TestNewWALReplicationServer(t *testing.T) {
	t.Run("success with temp dir", func(t *testing.T) {
		dataDir := newTempDir(t)
		defer cleanupDir(dataDir)

		server, err := newTestWALServer(t, dataDir)
		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}
		if server == nil {
			t.Fatal("Expected server instance, got nil")
		}
		if server.db == nil {
			t.Error("Expected Pebble DB to be initialized, got nil")
		}
		if server.zkManager == nil {
			t.Error("Expected ZKManager to be initialized, got nil")
		}

		// Check if the directory was created
		if _, err := os.Stat(dataDir); os.IsNotExist(err) {
			t.Errorf("Data directory %s was not created", dataDir)
		}

		// Clean up the server's DB as well
		if err := server.Close(); err != nil {
			t.Errorf("Failed to close server DB: %v", err)
		}
	})

	t.Run("error with empty DataDir", func(t *testing.T) {
		// Create a config with an empty DataDir
		cfg := WALConfig{
			NodeID:  "test-node",
			DataDir: "",
		}
		server, err := NewWALReplicationServer(cfg)
		if err == nil {
			t.Fatal("Expected error for empty DataDir, got nil")
		}
		if server != nil {
			t.Errorf("Expected nil server for empty DataDir, got %+v", server)
		}
		expectedErrSubstring := "DataDir must be specified"
		if !strings.Contains(err.Error(), expectedErrSubstring) {
			t.Errorf("Expected error message containing '%s', got '%s'", expectedErrSubstring, err.Error())
		}
	})

	// Test case for failure to create directory (e.g., invalid path or permissions)
	t.Run("error creating DataDir", func(t *testing.T) {
		// Use a path that's likely invalid or permissions-denied on most systems
		invalidDir := "/root/invalid/path/for/testing" // Requires root permissions to write
		if os.Getenv("USER") == "root" {
			t.Skip("Skipping directory creation error test when running as root")
		}
		cfg := WALConfig{
			NodeID:              "test-node-fail",
			InternalBindAddress: "127.0.0.1:0",
			DataDir:             invalidDir,
		}
		server, err := NewWALReplicationServer(cfg)
		if err == nil {
			t.Fatalf("Expected error for invalid DataDir, got nil")
		}
		if server != nil {
			t.Errorf("Expected nil server, got %+v", server)
		}
		expectedErrSubstring := "failed to create data directory"
		if !strings.Contains(err.Error(), expectedErrSubstring) {
			t.Errorf("Expected error message containing '%s', got '%s'", expectedErrSubstring, err.Error())
		}
	})
}

func TestWALReplicationServer_KVOperations(t *testing.T) {
	dataDir := newTempDir(t)
	defer cleanupDir(dataDir)

	server, err := newTestWALServer(t, dataDir)
	if err != nil {
		t.Fatalf("Failed to create server: %v", err)
	}
	defer server.Close()

	key1 := "testkey1"
	value1 := "testvalue1"
	key2 := "testkey2"
	value2 := "testvalue2"

	// Test Put
	t.Run("Put", func(t *testing.T) {
		err := server.Put(key1, value1)
		if err != nil {
			t.Fatalf("Failed to Put key %s: %v", key1, err)
		}
		t.Logf("Successfully put key: %s", key1)
	})

	// Test Get after Put
	t.Run("Get existing key", func(t *testing.T) {
		val, err := server.Get(key1)
		if err != nil {
			t.Fatalf("Failed to Get key %s: %v", key1, err)
		}
		if val != value1 {
			t.Errorf("Expected value '%s' for key '%s', got '%s'", value1, key1, val)
		}
		t.Logf("Successfully got key: %s, value: %s", key1, val)
	})

	// Test Get non-existing key
	t.Run("Get non-existing key", func(t *testing.T) {
		nonExistingKey := "nonexistentkey"
		_, err := server.Get(nonExistingKey)
		if err == nil {
			t.Fatalf("Expected error for non-existing key '%s', got nil", nonExistingKey)
		}
		expectedErrSubstring := "key not found"
		if !strings.Contains(err.Error(), expectedErrSubstring) {
			t.Errorf("Expected error message containing '%s', got '%s'", expectedErrSubstring, err.Error())
		}
		t.Logf("Correctly failed to get non-existing key: %s", nonExistingKey)
	})

	// Put another key
	t.Run("Put another key", func(t *testing.T) {
		err := server.Put(key2, value2)
		if err != nil {
			t.Fatalf("Failed to Put key %s: %v", key2, err)
		}
		t.Logf("Successfully put key: %s", key2)
	})

	// Test Delete
	t.Run("Delete existing key", func(t *testing.T) {
		err := server.Delete(key1)
		if err != nil {
			t.Fatalf("Failed to Delete key %s: %v", key1, err)
		}
		t.Logf("Successfully deleted key: %s", key1)
	})

	// Test Get after Delete
	t.Run("Get deleted key", func(t *testing.T) {
		_, err := server.Get(key1)
		if err == nil {
			t.Fatalf("Expected error for deleted key '%s', got nil", key1)
		}
		expectedErrSubstring := "key not found"
		if !strings.Contains(err.Error(), expectedErrSubstring) {
			t.Errorf("Expected error message containing '%s', got '%s'", expectedErrSubstring, err.Error())
		}
		t.Logf("Correctly failed to get deleted key: %s", key1)
	})

	// Test Delete non-existing key (should not error)
	t.Run("Delete non-existing key", func(t *testing.T) {
		nonExistingKey := "anothernonexistentkey"
		err := server.Delete(nonExistingKey)
		if err != nil {
			// Pebble's Delete does not return an error if the key doesn't exist.
			t.Fatalf("Expected no error when deleting non-existing key '%s', got %v", nonExistingKey, err)
		}
		t.Logf("Successfully (no error) deleted non-existing key: %s", nonExistingKey)
	})

	// Verify key2 is still there
	t.Run("Get unaffected key", func(t *testing.T) {
		val, err := server.Get(key2)
		if err != nil {
			t.Fatalf("Failed to Get key %s after other operations: %v", key2, err)
		}
		if val != value2 {
			t.Errorf("Expected value '%s' for key '%s', got '%s'", value2, key2, val)
		}
		t.Logf("Unaffected key %s still exists with value %s", key2, val)
	})
}

func TestWALReplicationServer_HTTP_KV(t *testing.T) {
	dataDir := newTempDir(t)
	defer cleanupDir(dataDir)

	// Create the server instance directly, without starting the real HTTP listener
	server, err := newTestWALServer(t, dataDir)
	if err != nil {
		t.Fatalf("Failed to create server: %v", err)
	}
	defer server.Close()

	// Setup the HTTP handler directly for testing by getting the mux from the server instance
	mux := server.setupWALHTTPServerMux()

	// Use httptest to create a test server
	ts := httptest.NewServer(mux)
	defer ts.Close()

	client := ts.Client()

	key1 := "httpkey1"
	value1 := "httpvalue1"
	key2 := "httpkey2"
	value2 := "httpvalue2"

	// Test PUT via HTTP
	t.Run("HTTP PUT", func(t *testing.T) {
		url := fmt.Sprintf("%s/kv?key=%s&value=%s", ts.URL, key1, value1)
		req, _ := http.NewRequest(http.MethodPut, url, nil)
		resp, err := client.Do(req)
		if err != nil {
			t.Fatalf("Failed to send PUT request: %v", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			body, _ := ioutil.ReadAll(resp.Body)
			t.Fatalf("Expected status %d, got %d. Body: %s", http.StatusOK, resp.StatusCode, string(body))
		}

		body, _ := ioutil.ReadAll(resp.Body)
		if !strings.Contains(string(body), "OK") {
			t.Errorf("Expected response body containing 'OK', got '%s'", string(body))
		}
		t.Logf("Successfully PUT key %s", key1)

		// Verify it exists in the underlying DB
		val, err := server.Get(key1)
		if err != nil {
			t.Errorf("Failed to get key %s directly from DB after HTTP PUT: %v", key1, err)
		}
		if val != value1 {
			t.Errorf("Value in DB after HTTP PUT mismatch. Expected '%s', got '%s'", value1, val)
		}
	})

	// Test GET via HTTP
	t.Run("HTTP GET existing key", func(t *testing.T) {
		url := fmt.Sprintf("%s/kv?key=%s", ts.URL, key1)
		resp, err := client.Get(url)
		if err != nil {
			t.Fatalf("Failed to send GET request: %v", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			body, _ := ioutil.ReadAll(resp.Body)
			t.Fatalf("Expected status %d, got %d. Body: %s", http.StatusOK, resp.StatusCode, string(body))
		}

		body, _ := ioutil.ReadAll(resp.Body)
		if string(body) != value1 {
			t.Errorf("Expected response body '%s', got '%s'", value1, string(body))
		}
		t.Logf("Successfully GET key %s with value %s", key1, string(body))
	})

	// Test GET non-existing key via HTTP
	t.Run("HTTP GET non-existing key", func(t *testing.T) {
		nonExistingKey := "httpnonexistent"
		url := fmt.Sprintf("%s/kv?key=%s", ts.URL, nonExistingKey)
		resp, err := client.Get(url)
		if err != nil {
			t.Fatalf("Failed to send GET request: %v", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusNotFound {
			body, _ := ioutil.ReadAll(resp.Body)
			t.Fatalf("Expected status %d, got %d. Body: %s", http.StatusNotFound, resp.StatusCode, string(body))
		}
		body, _ := ioutil.ReadAll(resp.Body)
		expectedErrSubstring := "key not found"
		if !strings.Contains(string(body), expectedErrSubstring) {
			t.Errorf("Expected response body containing '%s', got '%s'", expectedErrSubstring, string(body))
		}
		t.Logf("Correctly failed to GET non-existing key %s", nonExistingKey)
	})

	// Put another key for deletion test
	t.Run("HTTP PUT key for deletion", func(t *testing.T) {
		url := fmt.Sprintf("%s/kv?key=%s&value=%s", ts.URL, key2, value2)
		req, _ := http.NewRequest(http.MethodPut, url, nil)
		resp, err := client.Do(req)
		if err != nil {
			t.Fatalf("Failed to send PUT request: %v", err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			body, _ := ioutil.ReadAll(resp.Body)
			t.Fatalf("Expected status %d, got %d. Body: %s", http.StatusOK, resp.StatusCode, string(body))
		}
		t.Logf("Successfully PUT key %s for deletion test", key2)
	})

	// Test DELETE via HTTP
	t.Run("HTTP DELETE existing key", func(t *testing.T) {
		url := fmt.Sprintf("%s/kv?key=%s", ts.URL, key1)
		req, _ := http.NewRequest(http.MethodDelete, url, nil)
		resp, err := client.Do(req)
		if err != nil {
			t.Fatalf("Failed to send DELETE request: %v", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			body, _ := ioutil.ReadAll(resp.Body)
			t.Fatalf("Expected status %d, got %d. Body: %s", http.StatusOK, resp.StatusCode, string(body))
		}
		body, _ := ioutil.ReadAll(resp.Body)
		if !strings.Contains(string(body), "OK") {
			t.Errorf("Expected response body containing 'OK', got '%s'", string(body))
		}
		t.Logf("Successfully DELETE key %s", key1)

		// Verify it's deleted in the underlying DB
		_, err = server.Get(key1)
		if err == nil || !strings.Contains(err.Error(), "key not found") {
			t.Errorf("Expected 'key not found' error from DB after HTTP DELETE, got %v", err)
		}
	})

	// Test DELETE non-existing key via HTTP (should still return OK)
	t.Run("HTTP DELETE non-existing key", func(t *testing.T) {
		nonExistingKey := "httpanothernonexistent"
		url := fmt.Sprintf("%s/kv?key=%s", ts.URL, nonExistingKey)
		req, _ := http.NewRequest(http.MethodDelete, url, nil)
		resp, err := client.Do(req)
		if err != nil {
			t.Fatalf("Failed to send DELETE request: %v", err)
		}
		defer resp.Body.Close()

		// Pebble's Delete doesn't error if key not found, so the HTTP handler should return OK
		if resp.StatusCode != http.StatusOK {
			body, _ := ioutil.ReadAll(resp.Body)
			t.Fatalf("Expected status %d for deleting non-existing key, got %d. Body: %s", http.StatusOK, resp.StatusCode, string(body))
		}
		body, _ := ioutil.ReadAll(resp.Body)
		if !strings.Contains(string(body), "OK") {
			t.Errorf("Expected response body containing 'OK', got '%s'", string(body))
		}
		t.Logf("Successfully (no error) DELETE non-existing key %s via HTTP", nonExistingKey)
	})

	// Test missing key parameter
	t.Run("HTTP missing key parameter", func(t *testing.T) {
		url := fmt.Sprintf("%s/kv?value=%s", ts.URL, value1) // Missing key
		req, _ := http.NewRequest(http.MethodPut, url, nil)
		resp, err := client.Do(req)
		if err != nil {
			t.Fatalf("Failed to send PUT request: %v", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusBadRequest {
			t.Errorf("Expected status %d, got %d", http.StatusBadRequest, resp.StatusCode)
		}
		body, _ := ioutil.ReadAll(resp.Body)
		if !strings.Contains(string(body), "key parameter is required") {
			t.Errorf("Expected error message containing 'key parameter is required', got '%s'", string(body))
		}
		t.Log("Correctly handled missing key parameter")
	})

	// Test missing value parameter for PUT
	t.Run("HTTP missing value parameter for PUT", func(t *testing.T) {
		url := fmt.Sprintf("%s/kv?key=%s", ts.URL, key1) // Missing value
		req, _ := http.NewRequest(http.MethodPut, url, nil)
		resp, err := client.Do(req)
		if err != nil {
			t.Fatalf("Failed to send PUT request: %v", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusBadRequest {
			t.Errorf("Expected status %d, got %d", http.StatusBadRequest, resp.StatusCode)
		}
		body, _ := ioutil.ReadAll(resp.Body)
		if !strings.Contains(string(body), "value parameter is required for PUT") {
			t.Errorf("Expected error message containing 'value parameter is required for PUT', got '%s'", string(body))
		}
		t.Log("Correctly handled missing value parameter for PUT")
	})

	// Test disallowed method
	t.Run("HTTP disallowed method", func(t *testing.T) {
		url := fmt.Sprintf("%s/kv?key=%s", ts.URL, key1)
		req, _ := http.NewRequest(http.MethodPost, url, nil) // Using POST on /kv
		resp, err := client.Do(req)
		if err != nil {
			t.Fatalf("Failed to send POST request: %v", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusMethodNotAllowed {
			t.Errorf("Expected status %d, got %d", http.StatusMethodNotAllowed, resp.StatusCode)
		}
		body, _ := ioutil.ReadAll(resp.Body)
		if !strings.Contains(string(body), "Method not allowed") {
			t.Errorf("Expected error message containing 'Method not allowed', got '%s'", string(body))
		}
		t.Log("Correctly handled disallowed method POST on /kv")
	})
}

// TestWALReplicationServer_HTTP_StatusEndpoints tests the status endpoints like /wal/primary and /wal/stats.
func TestWALReplicationServer_HTTP_StatusEndpoints(t *testing.T) {
	dataDir := newTempDir(t)
	defer cleanupDir(dataDir)

	server, err := newTestWALServer(t, dataDir)
	if err != nil {
		t.Fatalf("Failed to create server: %v", err)
	}
	defer server.Close()

	// Setup the HTTP handler directly for testing
	mux := server.setupWALHTTPServerMux()
	ts := httptest.NewServer(mux)
	defer ts.Close()
	client := ts.Client()

	t.Run("HTTP /wal/primary", func(t *testing.T) {
		url := fmt.Sprintf("%s/wal/primary", ts.URL)
		resp, err := client.Get(url)
		if err != nil {
			t.Fatalf("Failed to send GET request: %v", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			t.Errorf("Expected status %d, got %d", http.StatusOK, resp.StatusCode)
		}
		body, _ := ioutil.ReadAll(resp.Body)
		// Since no ZK is configured, it should report not being primary and an error fetching the address.
		expectedPrimaryStatus := "Is this node primary: false\n"
		expectedPrimaryAddress := "Current Primary address: unknown (error fetching primary address)\n"

		if !strings.Contains(string(body), expectedPrimaryStatus) || !strings.Contains(string(body), expectedPrimaryAddress) {
			t.Errorf("Expected primary response to contain '%s' and '%s', got '%s'", expectedPrimaryStatus, expectedPrimaryAddress, string(body))
		}
	})

	t.Run("HTTP /wal/stats", func(t *testing.T) {
		url := fmt.Sprintf("%s/wal/stats", ts.URL)
		resp, err := client.Get(url)
		if err != nil {
			t.Fatalf("Failed to send GET request: %v", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			t.Errorf("Expected status %d, got %d", http.StatusOK, resp.StatusCode)
		}
		body, _ := ioutil.ReadAll(resp.Body)
		// Check for all the expected placeholder stats fields
		// Check for all the expected stats fields from the updated GetStats
		expectedStrings := []string{
			"status: running",

			"node_id: test-node", // Matches config in newTestWALServer
			fmt.Sprintf("data_dir: %s", server.config.DataDir),
			fmt.Sprintf("http_address: %s", server.config.HTTPAddr),
			fmt.Sprintf("internal_address: %s", server.config.InternalBindAddress),
			"is_primary: false", // In test setup, ZK is not fully functional for primary election
			"current_primary: unknown (error fetching primary address)", // Same as above
		}

		bodyStr := string(body)
		for _, expected := range expectedStrings {
			if !strings.Contains(bodyStr, expected) {
				t.Errorf("Expected stats to contain '%s', but got '%s'", expected, bodyStr)
				break // Fail fast if one check fails
			}
		}
	})
}
