package filesys

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
)

type Node struct {
	Address string
	Port    int
}

type FileMetadata struct {
	FileName string
	Size     int64
	Node     Node
}

type SimpleDistributedFileSystem struct {
	metadata map[string]FileMetadata
	nodes    []Node
	client   *http.Client
}

func NewSimpleDistributedFileSystem(nodes []Node) *SimpleDistributedFileSystem {
	return &SimpleDistributedFileSystem{
		metadata: make(map[string]FileMetadata),
		nodes:    nodes,
		client:   &http.Client{},
	}
}

func (s *SimpleDistributedFileSystem) Put(file *os.File) error {
	fileInfo, err := file.Stat()
	if err != nil {
		return err
	}

	node := s.nodes[len(s.metadata)%len(s.nodes)]

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

func (s *SimpleDistributedFileSystem) Get(fileName string) (*os.File, error) {
	metadata, ok := s.metadata[fileName]
	if !ok {
		return nil, errors.New("file not found")
	}

	url := fmt.Sprintf("http://%s:%d/download/%s", metadata.Node.Address, metadata.Node.Port, fileName)
	resp, err := s.client.Get(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, errors.New("failed to download file")
	}

	tempFile, err := os.CreateTemp("", fileName)
	if err != nil {
		return nil, err
	}

	_, err = io.Copy(tempFile, resp.Body)
	return tempFile, err
}
