package wal_replicator

import (
	"fmt"
	"log"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/go-zookeeper/zk"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

// MockZKConn is a mock implementation of the zk.Conn interface.
type MockZKConn struct {
	mock.Mock
	// Add a mutex to protect channels if they are accessed concurrently by the mock and test goroutines
	childrenWatchChan map[string]chan zk.Event
	existsWatchChan   map[string]chan zk.Event
	mu                sync.Mutex
}

func NewMockZKConn() *MockZKConn {
	return &MockZKConn{
		childrenWatchChan: make(map[string]chan zk.Event),
		existsWatchChan:   make(map[string]chan zk.Event),
	}
}

func (m *MockZKConn) Create(path string, data []byte, flags int32, acl []zk.ACL) (string, error) {
	args := m.Called(path, data, flags, acl)
	return args.String(0), args.Error(1)
}

func (m *MockZKConn) Exists(path string) (bool, *zk.Stat, error) {
	args := m.Called(path)
	return args.Bool(0), nil, args.Error(2)
}

func (m *MockZKConn) Get(path string) ([]byte, *zk.Stat, error) {
	args := m.Called(path)
	return args.Get(0).([]byte), nil, args.Error(2)
}

func (m *MockZKConn) Children(path string) ([]string, *zk.Stat, error) {
	args := m.Called(path)
	return args.Get(0).([]string), nil, args.Error(2)
}

func (m *MockZKConn) Delete(path string, version int32) error {
	args := m.Called(path, version)
	return args.Error(0)
}

func (m *MockZKConn) ChildrenW(path string) ([]string, *zk.Stat, <-chan zk.Event, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	args := m.Called(path)
	eventChan := make(chan zk.Event, 1) // Buffered channel for the test
	m.childrenWatchChan[path] = eventChan
	return args.Get(0).([]string), nil, eventChan, args.Error(3)
}

func (m *MockZKConn) ExistsW(path string) (bool, *zk.Stat, <-chan zk.Event, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	args := m.Called(path)
	eventChan := make(chan zk.Event, 1) // Buffered channel for the test
	m.existsWatchChan[path] = eventChan
	return args.Bool(0), nil, eventChan, args.Error(3)
}

func (m *MockZKConn) Close() {
	m.Called()
}

// FireChildrenWatchEvent simulates a ZooKeeper event on a ChildrenW watch.
func (m *MockZKConn) FireChildrenWatchEvent(path string, eventType zk.EventType) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if ch, ok := m.childrenWatchChan[path]; ok && ch != nil {
		ch <- zk.Event{Type: eventType, Path: path}
	}
}

// FireExistsWatchEvent simulates a ZooKeeper event on an ExistsW watch.
func (m *MockZKConn) FireExistsWatchEvent(path string, eventType zk.EventType) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if ch, ok := m.existsWatchChan[path]; ok && ch != nil {
		ch <- zk.Event{Type: eventType, Path: path}
	}
}

func TestNewZKManager(t *testing.T) {
	logger := log.New(os.Stdout, "TEST: ", log.LstdFlags)
	nodeID := "test-node-1"
	callback := func(nodes map[string]string) {
		// The callback's invocation and data are tested in TestZKManager_watchActiveNodes.
		// In NewZKManager, we only assert its presence.
	}

	zkm := NewZKManager(logger, nodeID, callback)

	assert.NotNil(t, zkm)
	assert.Equal(t, logger, zkm.logger)
	assert.Equal(t, nodeID, zkm.nodeID)
	assert.NotNil(t, zkm.activeNodes)
	assert.Equal(t, 0, len(zkm.activeNodes))
	assert.NotNil(t, zkm.nodeChangeCallback)

	// Test if callback is nil when passed as nil
	zkmNoCallback := NewZKManager(logger, nodeID, nil)
	assert.Nil(t, zkmNoCallback.nodeChangeCallback)
}

func TestZKManager_Connect(t *testing.T) {
	logger := log.New(os.Stdout, "TEST: ", log.LstdFlags)

	// Test 1: Successful connection and path creation
	t.Run("SuccessfulConnectionAndPathCreation", func(t *testing.T) {
		mockConn := NewMockZKConn()
		zkm := NewZKManager(logger, "test-node-1", nil)

		// Override the connectFunc to return our mock connection
		zkm.connectFunc = func(servers []string, recvTimeout time.Duration) (ZKConnection, <-chan zk.Event, error) {
			return mockConn, make(chan zk.Event), nil // Return mockConn and a dummy channel
		}

		// Expectations for base path existence and creation
		mockConn.On("Exists", mock.AnythingOfType("string")).Return(false, nil, nil).Times(4) // For all 4 paths
		mockConn.On("Create", mock.AnythingOfType("string"), mock.Anything, int32(0), zk.WorldACL(zk.PermAll)).Return("/path", nil).Times(4)

		err := zkm.Connect([]string{"localhost:2181"})
		assert.NoError(t, err)
		assert.Equal(t, mockConn, zkm.conn) // Ensure the mock conn is set by connectFunc

		mockConn.AssertExpectations(t)
	})

	// Test 2: Paths already exist
	t.Run("PathsAlreadyExist", func(t *testing.T) {
		mockConn := NewMockZKConn()
		zkm := NewZKManager(logger, "test-node-2", nil)

		// Override the connectFunc to return our mock connection
		zkm.connectFunc = func(servers []string, recvTimeout time.Duration) (ZKConnection, <-chan zk.Event, error) {
			return mockConn, make(chan zk.Event), nil
		}

		// Expectations: Exists returns true, Create should not be called
		mockConn.On("Exists", mock.AnythingOfType("string")).Return(true, nil, nil).Times(4)

		err := zkm.Connect([]string{"localhost:2181"})
		assert.NoError(t, err)
		assert.Equal(t, mockConn, zkm.conn)

		mockConn.AssertExpectations(t)
	})

	// Test 3: Connection failure
	t.Run("ConnectionFailure", func(t *testing.T) {
		zkm := NewZKManager(logger, "test-node-3", nil)

		// Override the connectFunc to simulate connection error
		expectedErr := fmt.Errorf("simulated connection error")
		zkm.connectFunc = func(servers []string, recvTimeout time.Duration) (ZKConnection, <-chan zk.Event, error) {
			return nil, nil, expectedErr
		}

		err := zkm.Connect([]string{"invalid-zk-address"})
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to connect to zookeeper")
		assert.Nil(t, zkm.conn) // Conn should remain nil on failure
	})
}

func TestZKManager_Close(t *testing.T) {
	logger := log.New(os.Stdout, "TEST: ", log.LstdFlags)
	zkm := NewZKManager(logger, "test-node", nil)

	mockConn := NewMockZKConn()
	zkm.conn = mockConn // Manually set the mock connection

	mockConn.On("Close").Return().Once()

	zkm.Close()
	mockConn.AssertExpectations(t)
	assert.Nil(t, zkm.conn) // Connection should be nil after close

	// Test calling close on nil connection
	zkm.conn = nil
	zkm.Close() // Should not panic or error
}

func TestZKManager_RegisterNode(t *testing.T) {
	logger := log.New(os.Stdout, "TEST: ", log.LstdFlags)
	zkm := NewZKManager(logger, "test-node-1", nil)
	zkm.internalBindAddress = "127.0.0.1:8001"

	mockConn := NewMockZKConn()
	zkm.conn = mockConn

	nodePath := fmt.Sprintf("%s/%s", nodesPath, zkm.nodeID)

	// Test successful registration (node does not exist initially)
	mockConn.On("Exists", nodePath).Return(false, nil, nil).Once()
	mockConn.On("Create", nodePath, []byte(zkm.internalBindAddress), int32(zk.FlagEphemeral), zk.WorldACL(zk.PermAll)).Return(nodePath, nil).Once()

	err := zkm.RegisterNode()
	assert.NoError(t, err)
	mockConn.AssertExpectations(t)

	// Test registration when node already exists, then deleted and recreated
	mockConn = NewMockZKConn()
	zkm.conn = mockConn
	mockConn.On("Exists", nodePath).Return(true, nil, nil).Once()
	mockConn.On("Delete", nodePath, int32(-1)).Return(nil).Once()
	mockConn.On("Create", nodePath, []byte(zkm.internalBindAddress), int32(zk.FlagEphemeral), zk.WorldACL(zk.PermAll)).Return(nodePath, nil).Once()

	err = zkm.RegisterNode()
	assert.NoError(t, err)
	mockConn.AssertExpectations(t)

	// Test registration when node exists and delete fails (but create succeeds)
	mockConn = NewMockZKConn()
	zkm.conn = mockConn
	mockConn.On("Exists", nodePath).Return(true, nil, nil).Once()
	mockConn.On("Delete", nodePath, int32(-1)).Return(zk.ErrNoNode).Once() // Simulate another process deleting it
	mockConn.On("Create", nodePath, []byte(zkm.internalBindAddress), int32(zk.FlagEphemeral), zk.WorldACL(zk.PermAll)).Return(nodePath, nil).Once()

	err = zkm.RegisterNode()
	assert.NoError(t, err) // Should still succeed as create will be tried
	mockConn.AssertExpectations(t)

	// Test registration failure (Create fails)
	mockConn = NewMockZKConn()
	zkm.conn = mockConn
	mockConn.On("Exists", nodePath).Return(false, nil, nil).Once()
	mockConn.On("Create", nodePath, []byte(zkm.internalBindAddress), int32(zk.FlagEphemeral), zk.WorldACL(zk.PermAll)).Return("", assert.AnError).Once()

	err = zkm.RegisterNode()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to create ephemeral node")
	mockConn.AssertExpectations(t)

	// Test registration with nil connection
	zkm.conn = nil
	err = zkm.RegisterNode()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "ZK connection is nil")

	// Test registration with empty internalBindAddress
	zkm.conn = mockConn // restore mock
	zkm.internalBindAddress = ""
	err = zkm.RegisterNode()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "internalBindAddress not set")
}

func TestZKManager_watchActiveNodes(t *testing.T) {
	logger := log.New(os.Stdout, "TEST: ", log.LstdFlags)
	var mu sync.Mutex
	var receivedNodes map[string]string
	callback := func(nodes map[string]string) {
		mu.Lock()
		receivedNodes = nodes
		mu.Unlock()
	}
	zkm := NewZKManager(logger, "test-node-1", callback)

	mockConn := NewMockZKConn()
	zkm.conn = mockConn

	// Simulate some active nodes
	mockNodes := []string{"node-A", "node-B"}
	mockNodeAData := []byte("192.168.1.1:8001")
	mockNodeBData := []byte("192.168.1.2:8002")

	mockConn.On("ChildrenW", nodesPath).Return(mockNodes, nil, make(<-chan zk.Event), nil).Once()
	mockConn.On("Get", fmt.Sprintf("%s/%s", nodesPath, "node-A")).Return(mockNodeAData, nil, nil).Once()
	mockConn.On("Get", fmt.Sprintf("%s/%s", nodesPath, "node-B")).Return(mockNodeBData, nil, nil).Once()

	eventChan, err := zkm.watchActiveNodes()
	assert.NoError(t, err)
	assert.NotNil(t, eventChan)
	mockConn.AssertExpectations(t)

	// Check if activeNodes was updated
	expectedActiveNodes := map[string]string{
		"node-A": "192.168.1.1:8001",
		"node-B": "192.168.1.2:8002",
	}
	assert.Equal(t, expectedActiveNodes, zkm.GetActiveNodes())

	// Check if callback was invoked with correct data
	mu.Lock()
	assert.Equal(t, expectedActiveNodes, receivedNodes)
	mu.Unlock()

	// Test ChildrenW error
	mockConn = NewMockZKConn()
	zkm.conn = mockConn
	mockConn.On("ChildrenW", nodesPath).Return([]string{}, nil, make(<-chan zk.Event), assert.AnError).Once()

	eventChan, err = zkm.watchActiveNodes()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to watch active nodes")
	assert.Nil(t, eventChan)
	mockConn.AssertExpectations(t)
}

func TestZKManager_ElectPrimary(t *testing.T) {
	logger := log.New(os.Stdout, "TEST: ", log.LstdFlags)
	zkm := NewZKManager(logger, "node-C", nil)
	zkm.internalBindAddress = "127.0.0.1:8003"
	mockConn := NewMockZKConn()
	zkm.conn = mockConn

	baseElectionPath := "/kvreplicator/wal/primary_election"

	// Scenario 1: This node becomes primary
	t.Run("Node becomes primary", func(t *testing.T) {
		mockConn = NewMockZKConn()
		zkm.conn = mockConn
		zkm.electionNodePath = "" // Ensure it's fresh

		// Expectations for creating election node
		mockConn.On("Exists", mock.AnythingOfType("string")).Return(false, nil, nil).Maybe() // For existing election node check
		mockConn.On("Create", fmt.Sprintf("%s/node-", baseElectionPath), []byte(zkm.internalBindAddress), int32(zk.FlagEphemeral|zk.FlagSequence), zk.WorldACL(zk.PermAll)).Return(baseElectionPath+"/node-0000000000", nil).Once()

		// Expectations for getting children (this node is the first)
		mockConn.On("Children", baseElectionPath).Return([]string{"node-0000000000", "node-0000000001", "node-0000000002"}, nil, nil).Once()

		// Expectations for primary (setting ChildrenW watch)
		mockConn.On("ChildrenW", baseElectionPath).Return([]string{}, nil, make(<-chan zk.Event), nil).Once()

		isPrimary, primaryAddr, eventChan, err := zkm.ElectPrimary()
		assert.NoError(t, err)
		assert.True(t, isPrimary)
		assert.Equal(t, zkm.internalBindAddress, primaryAddr)
		assert.NotNil(t, eventChan)
		assert.Equal(t, baseElectionPath+"/node-0000000000", zkm.electionNodePath)
		mockConn.AssertExpectations(t)
	})

	// Scenario 2: This node is not primary, watches predecessor
	t.Run("Node is not primary, watches predecessor", func(t *testing.T) {
		mockConn = NewMockZKConn()
		zkm.conn = mockConn
		zkm.electionNodePath = "" // Ensure it's fresh
		zkm.nodeID = "node-C"
		zkm.internalBindAddress = "127.0.0.1:8003"

		// Mock children such that our node is not the first
		electionNodes := []string{"node-0000000000", "node-0000000001", "node-0000000002"}
		primaryNodePath := fmt.Sprintf("%s/%s", baseElectionPath, electionNodes[0])
		myNodePath := fmt.Sprintf("%s/%s", baseElectionPath, electionNodes[2])
		predecessorNodePath := fmt.Sprintf("%s/%s", baseElectionPath, electionNodes[1])

		// Expectations for creating election node
		mockConn.On("Exists", mock.AnythingOfType("string")).Return(false, nil, nil).Maybe()
		mockConn.On("Create", fmt.Sprintf("%s/node-", baseElectionPath), []byte(zkm.internalBindAddress), int32(zk.FlagEphemeral|zk.FlagSequence), zk.WorldACL(zk.PermAll)).Return(myNodePath, nil).Once()

		// Expectations for getting children
		mockConn.On("Children", baseElectionPath).Return(electionNodes, nil, nil).Once()

		// Expectation for getting primary data
		mockConn.On("Get", primaryNodePath).Return([]byte("127.0.0.1:8001"), nil, nil).Once()

		// Expectation for watching predecessor
		mockConn.On("ExistsW", predecessorNodePath).Return(true, nil, make(<-chan zk.Event), nil).Once()

		isPrimary, primaryAddr, eventChan, err := zkm.ElectPrimary()
		assert.NoError(t, err)
		assert.False(t, isPrimary)
		assert.Equal(t, "127.0.0.1:8001", primaryAddr)
		assert.NotNil(t, eventChan)
		assert.Equal(t, myNodePath, zkm.electionNodePath)
		mockConn.AssertExpectations(t)
	})

	// Scenario 3: Predecessor node disappears before watch can be set (should re-elect)
	t.Run("Predecessor gone, re-elects", func(t *testing.T) {
		mockConn = NewMockZKConn()
		zkm.conn = mockConn
		zkm.electionNodePath = "" // Ensure it's fresh
		zkm.nodeID = "node-B"
		zkm.internalBindAddress = "127.0.0.1:8002"

		electionNodes := []string{"node-0000000000", "node-0000000001"} // Primary and our node
		primaryNodePath := fmt.Sprintf("%s/%s", baseElectionPath, electionNodes[0])
		myNodePath := fmt.Sprintf("%s/%s", baseElectionPath, electionNodes[1])

		// First call to ElectPrimary:
		// Create node
		mockConn.On("Create", fmt.Sprintf("%s/node-", baseElectionPath), []byte(zkm.internalBindAddress), int32(zk.FlagEphemeral|zk.FlagSequence), zk.WorldACL(zk.PermAll)).Return(myNodePath, nil).Once()
		// Get children
		mockConn.On("Children", baseElectionPath).Return(electionNodes, nil, nil).Once()
		// Get primary data
		mockConn.On("Get", primaryNodePath).Return([]byte("127.0.0.1:8001"), nil, nil).Once()
		// Watch predecessor (node-0000000000) - but it's already gone!
		mockConn.On("ExistsW", primaryNodePath).Return(false, nil, make(<-chan zk.Event), nil).Once()

		// Second call to ElectPrimary (recursive call due to predecessor gone):
		// This node now becomes primary. No new create call because electionNodePath is already set.
		mockConn.On("Exists", myNodePath).Return(true, nil, nil).Once()                                                   // Check if our node is still valid
		mockConn.On("Children", baseElectionPath).Return([]string{myNodePath[len(baseElectionPath)+1:]}, nil, nil).Once() // Now only our node exists
		mockConn.On("ChildrenW", baseElectionPath).Return([]string{}, nil, make(<-chan zk.Event), nil).Once()

		isPrimary, primaryAddr, eventChan, err := zkm.ElectPrimary()
		assert.NoError(t, err)
		assert.True(t, isPrimary)
		assert.Equal(t, zkm.internalBindAddress, primaryAddr)
		assert.NotNil(t, eventChan)
		mockConn.AssertExpectations(t)
	})

	// Scenario 4: Children() returns 0 children, should retry
	t.Run("No children, retries", func(t *testing.T) {
		mockConn = NewMockZKConn()
		zkm.conn = mockConn
		zkm.electionNodePath = "" // Ensure it's fresh
		zkm.nodeID = "node-A"
		zkm.internalBindAddress = "127.0.0.1:8001"

		myNodePath := fmt.Sprintf("%s/node-0000000000", baseElectionPath)

		// First Children call: return empty, triggers retry
		mockConn.On("Exists", mock.AnythingOfType("string")).Return(false, nil, nil).Maybe()
		mockConn.On("Create", fmt.Sprintf("%s/node-", baseElectionPath), mock.Anything, mock.Anything, mock.Anything).Return(myNodePath, nil).Times(2)
		mockConn.On("Children", baseElectionPath).Return([]string{}, nil, nil).Once() // First attempt: no children

		// Second Children call (after retry): return children, this node is primary
		mockConn.On("Children", baseElectionPath).Return([]string{"node-0000000000"}, nil, nil).Once()
		mockConn.On("ChildrenW", baseElectionPath).Return([]string{}, nil, make(<-chan zk.Event), nil).Once()

		isPrimary, primaryAddr, eventChan, err := zkm.ElectPrimary()
		assert.NoError(t, err)
		assert.True(t, isPrimary)
		assert.Equal(t, zkm.internalBindAddress, primaryAddr)
		assert.NotNil(t, eventChan)
		mockConn.AssertExpectations(t)
	})

	// Scenario 5: Error on Children() call
	t.Run("Children error", func(t *testing.T) {
		mockConn = NewMockZKConn()
		zkm.conn = mockConn
		zkm.electionNodePath = "" // Ensure it's fresh
		zkm.nodeID = "node-X"
		zkm.internalBindAddress = "127.0.0.1:9000"

		myNodePath := fmt.Sprintf("%s/node-0000000000", baseElectionPath)

		mockConn.On("Exists", mock.AnythingOfType("string")).Return(false, nil, nil).Maybe()
		mockConn.On("Create", fmt.Sprintf("%s/node-", baseElectionPath), mock.Anything, mock.Anything, mock.Anything).Return(myNodePath, nil).Once()
		mockConn.On("Children", baseElectionPath).Return([]string{}, nil, assert.AnError).Once()

		isPrimary, primaryAddr, eventChan, err := zkm.ElectPrimary()
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to get election path children")
		assert.False(t, isPrimary)
		assert.Empty(t, primaryAddr)
		assert.Nil(t, eventChan)
		mockConn.AssertExpectations(t)
	})

	// Scenario 6: ZK connection is nil
	t.Run("Nil ZK connection", func(t *testing.T) {
		zkm.conn = nil
		isPrimary, primaryAddr, eventChan, err := zkm.ElectPrimary()
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "zookeeper connection not established")
		assert.False(t, isPrimary)
		assert.Empty(t, primaryAddr)
		assert.Nil(t, eventChan)
	})
}

func TestZKManager_handleZkEvents(t *testing.T) {
	logger := log.New(os.Stdout, "TEST: ", log.LstdFlags)
	zkm := NewZKManager(logger, "test-node", nil)
	zkm.internalBindAddress = "127.0.0.1:8001" // Needed for re-registration

	mockConn := NewMockZKConn()
	zkm.conn = mockConn

	electionEventChan := make(chan zk.Event, 5)
	nodeEventChan := make(chan zk.Event, 5)

	// Mock for handleElectionEvent's ElectPrimary call
	// This will be called when an election event needs a re-election
	mockConn.On("Exists", mock.AnythingOfType("string")).Return(true, nil, nil).Maybe() // For election node validation
	mockConn.On("Create", mock.AnythingOfType("string"), mock.Anything, mock.Anything, mock.Anything).Return("/kvreplicator/wal/primary_election/node-0000000000", nil).Maybe()
	mockConn.On("Children", primaryElectionPath).Return([]string{"node-0000000000"}, nil, nil).Maybe()
	mockConn.On("ChildrenW", primaryElectionPath).Return([]string{}, nil, make(<-chan zk.Event), nil).Maybe()
	mockConn.On("Get", mock.AnythingOfType("string")).Return([]byte("addr"), nil, nil).Maybe() // For GetPrimaryInfo if called

	// Mock for handleNodesEvent's watchActiveNodes call
	mockConn.On("ChildrenW", nodesPath).Return([]string{"node-1"}, nil, make(<-chan zk.Event), nil).Maybe()
	mockConn.On("Get", fmt.Sprintf("%s/%s", nodesPath, "node-1")).Return([]byte("127.0.0.1:8001"), nil, nil).Maybe()
	mockConn.On("Exists", fmt.Sprintf("%s/%s", nodesPath, zkm.nodeID)).Return(false, nil, nil).Maybe() // For re-registration
	mockConn.On("Delete", fmt.Sprintf("%s/%s", nodesPath, zkm.nodeID), int32(-1)).Return(nil).Maybe()
	mockConn.On("Create", fmt.Sprintf("%s/%s", nodesPath, zkm.nodeID), []byte(zkm.internalBindAddress), zk.FlagEphemeral, zk.WorldACL(zk.PermAll)).Return(fmt.Sprintf("%s/%s", nodesPath, zkm.nodeID), nil).Maybe()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		zkm.handleZkEvents(electionEventChan, nodeEventChan)
	}()

	// Send a node change event
	nodeEventChan <- zk.Event{Type: zk.EventNodeChildrenChanged, Path: nodesPath}
	time.Sleep(10 * time.Millisecond) // Give goroutine time to process

	// Send an election event
	electionEventChan <- zk.Event{Type: zk.EventNodeDeleted, Path: "/some/election/node"}
	time.Sleep(10 * time.Millisecond) // Give goroutine time to process

	// Test session expired for nodes
	nodeEventChan <- zk.Event{Type: zk.EventSession, State: zk.StateExpired}
	time.Sleep(10 * time.Millisecond) // Give goroutine time to process

	// Test session expired for election
	electionEventChan <- zk.Event{Type: zk.EventSession, State: zk.StateExpired}
	time.Sleep(10 * time.Millisecond) // Give goroutine time to process

	// Close channels to signal goroutine to exit
	close(electionEventChan)
	close(nodeEventChan)

	wg.Wait() // Wait for the handler goroutine to finish

	// Verify that the mocked methods were called as expected
	mockConn.AssertCalled(t, "ChildrenW", nodesPath)          // From watchActiveNodes call after node event
	mockConn.AssertCalled(t, "Children", primaryElectionPath) // From ElectPrimary call after election event
	mockConn.AssertCalled(t, "RegisterNode")                  // From handleNodesEvent's session expired
}

func TestZKManager_GetPrimaryInfo(t *testing.T) {
	logger := log.New(os.Stdout, "TEST: ", log.LstdFlags)
	zkm := NewZKManager(logger, "test-node-1", nil)
	zkm.internalBindAddress = "127.0.0.1:8001"
	mockConn := NewMockZKConn()
	zkm.conn = mockConn

	baseElectionPath := "/kvreplicator/wal/primary_election"

	// Scenario 1: Primary exists and it's this node
	t.Run("This node is primary", func(t *testing.T) {
		mockConn = NewMockZKConn()
		zkm.conn = mockConn
		zkm.electionNodePath = baseElectionPath + "/node-0000000000" // Assume this node is the first

		mockConn.On("Children", baseElectionPath).Return([]string{"node-0000000000", "node-0000000001"}, nil, nil).Once()
		mockConn.On("Get", baseElectionPath+"/node-0000000000").Return([]byte(zkm.internalBindAddress), nil, nil).Once()

		isPrimary, primaryAddr, err := zkm.GetPrimaryInfo()
		assert.NoError(t, err)
		assert.True(t, isPrimary)
		assert.Equal(t, zkm.internalBindAddress, primaryAddr)
		mockConn.AssertExpectations(t)
	})

	// Scenario 2: Primary exists but it's another node
	t.Run("Another node is primary", func(t *testing.T) {
		mockConn = NewMockZKConn()
		zkm.conn = mockConn
		zkm.electionNodePath = baseElectionPath + "/node-0000000001" // Assume this node is second

		mockConn.On("Children", baseElectionPath).Return([]string{"node-0000000000", "node-0000000001"}, nil, nil).Once()
		mockConn.On("Get", baseElectionPath+"/node-0000000000").Return([]byte("127.0.0.1:8000"), nil, nil).Once()

		isPrimary, primaryAddr, err := zkm.GetPrimaryInfo()
		assert.NoError(t, err)
		assert.False(t, isPrimary)
		assert.Equal(t, "127.0.0.1:8000", primaryAddr)
		mockConn.AssertExpectations(t)
	})

	// Scenario 3: No primary elected (empty children)
	t.Run("No primary elected", func(t *testing.T) {
		mockConn = NewMockZKConn()
		zkm.conn = mockConn
		zkm.electionNodePath = "" // No election node yet for this instance

		mockConn.On("Children", baseElectionPath).Return([]string{}, nil, nil).Once()

		isPrimary, primaryAddr, err := zkm.GetPrimaryInfo()
		assert.NoError(t, err)
		assert.False(t, isPrimary)
		assert.Empty(t, primaryAddr)
		mockConn.AssertExpectations(t)
	})

	// Scenario 4: Error getting children
	t.Run("Children error", func(t *testing.T) {
		mockConn = NewMockZKConn()
		zkm.conn = mockConn
		zkm.electionNodePath = baseElectionPath + "/node-0000000000"

		mockConn.On("Children", baseElectionPath).Return([]string{}, nil, assert.AnError).Once()

		isPrimary, primaryAddr, err := zkm.GetPrimaryInfo()
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to get children for primary election path")
		assert.False(t, isPrimary)
		assert.Empty(t, primaryAddr)
		mockConn.AssertExpectations(t)
	})

	// Scenario 5: Error getting primary data
	t.Run("Get primary data error", func(t *testing.T) {
		mockConn = NewMockZKConn()
		zkm.conn = mockConn
		zkm.electionNodePath = baseElectionPath + "/node-0000000000"

		mockConn.On("Children", baseElectionPath).Return([]string{"node-0000000000"}, nil, nil).Once()
		mockConn.On("Get", baseElectionPath+"/node-0000000000").Return(nil, nil, assert.AnError).Once()

		isPrimary, primaryAddr, err := zkm.GetPrimaryInfo()
		assert.NoError(t, err) // Error is swallowed and primaryAddr set to "unknown"
		assert.True(t, isPrimary)
		assert.Equal(t, "unknown (error fetching primary address)", primaryAddr)
		mockConn.AssertExpectations(t)
	})

	// Scenario 6: ZK connection is nil
	t.Run("Nil ZK connection", func(t *testing.T) {
		zkm.conn = nil
		isPrimary, primaryAddr, err := zkm.GetPrimaryInfo()
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "zookeeper connection not established")
		assert.False(t, isPrimary)
		assert.Empty(t, primaryAddr)
	})
}

// Ensure the TestMain is present for general setup if needed
func TestMain(m *testing.M) {
	// You might want to suppress logs during tests
	log.SetOutput(os.Stderr)
	log.SetFlags(log.Ldate | log.Ltime | log.Lshortfile)
	os.Exit(m.Run())
}
