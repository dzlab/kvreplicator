```go
package kvreplicator

import (
	"encoding/json"
	"fmt"
)

// OpType represents the type of operation to be performed.
type OpType string

const (
	// OpPut represents a set/put operation.
	OpPut OpType = "PUT"
	// OpDelete represents a delete operation.
	OpDelete OpType = "DELETE"
)

// Command represents a command to be applied to the key-value store.
// This is what gets written to the RAFT log.
type Command struct {
	Op    OpType `json:"op"`
	Key   string `json:"key"`
	Value string `json:"value,omitempty"` // Value is optional for delete operations
}

// Serialize converts the command to a byte slice for RAFT log storage.
func (c *Command) Serialize() ([]byte, error) {
	return json.Marshal(c)
}

// Deserialize attempts to convert a byte slice back into a Command.
func DeserializeCommand(data []byte) (*Command, error) {
	if len(data) == 0 {
		return nil, fmt.Errorf("cannot deserialize empty data")
	}
	var cmd Command
	if err := json.Unmarshal(data, &cmd); err != nil {
		return nil, fmt.Errorf("failed to unmarshal command: %w", err)
	}
	return &cmd, nil
}
```