// Filesys package is a simple distributed file system (dfs) implementation
// demonstrating one of the Web3 features
// TO-DO: implement remaining CRUD operations
package filesys

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
)

// Stores Node info
type Node struct {
	Address string
	Port    int
}

// Stores File metadata
type FileMetadata struct {
	FileName string
	Size     int64
	Node     Node
}

// Stores an array of connected nodes and its file metadata
type SimpleDistributedFileSystem struct {
	metadata map[string]FileMetadata
	nodes    []Node
	client   *http.Client
}

// Starts a new File system component (exposed constructor)
func NewSimpleDistributedFileSystem(nodes []Node) *SimpleDistributedFileSystem {
	return &SimpleDistributedFileSystem{
		metadata: make(map[string]FileMetadata),
		nodes:    nodes,
		client:   &http.Client{},
	}
}

// Broadcast upload request to connected Nodes
func (s *SimpleDistributedFileSystem) Put(file *os.File) error {

	// Collects file info from os.File
	fileInfo, err := file.Stat()
	if err != nil {
		return err
	}

	// Loads nodes file's metadata
	node := s.nodes[len(s.metadata)%len(s.nodes)]

	// Formats PUT http request (upload)
	url := fmt.Sprintf("http://%s:%d/upload/%s", node.Address, node.Port, fileInfo.Name())
	req, err := http.NewRequest(http.MethodPut, url, file)
	if err != nil {
		return err
	}

	resp, err := s.client.Do(req)
	if err != nil {
		return err
	}

	if resp.StatusCode != http.StatusOK {
		return errors.New("failed to upload file")
	}

	s.metadata[fileInfo.Name()] = FileMetadata{
		FileName: fileInfo.Name(),
		Size:     fileInfo.Size(),
		Node:     node,
	}
	return nil
}

// Broadcast download request to connected Nodes
func (s *SimpleDistributedFileSystem) Get(fileName string) (*os.File, error) {

	// Collects file info from os.Files
	metadata, ok := s.metadata[fileName]
	if !ok {
		return nil, errors.New("file not found")
	}

	// Formats GET http request (download)
	url := fmt.Sprintf("http://%s:%d/download/%s", metadata.Node.Address, metadata.Node.Port, fileName)
	resp, err := s.client.Get(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, errors.New("failed to download file")
	}

	// Creates the file locally (download)
	tempFile, err := os.CreateTemp("", fileName)
	if err != nil {
		return nil, err
	}

	_, err = io.Copy(tempFile, resp.Body)
	return tempFile, err
}
