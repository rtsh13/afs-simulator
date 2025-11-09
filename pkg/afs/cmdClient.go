package afs

import (
	"fmt"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"sync"

	"github.com/afs-simulator/pkg/utils"
)

type AFSClient struct {
	clientID   string
	cacheDir   string
	serverAddr string

	rpcClient *rpc.Client

	// Cache metadata
	cacheMutex sync.RWMutex
	cacheInfo  map[string]*CachedFile
}

type CachedFile struct {
	LocalPath string
	Version   int64
	IsDirty   bool
}

func NewAFSClient(clientID, cacheDir, serverAddr string) (*AFSClient, error) {
	// Create cache directory
	if err := os.MkdirAll(cacheDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create cache dir: %v", err)
	}

	// Connect to server
	client, err := rpc.Dial("tcp", serverAddr)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to server: %v", err)
	}

	afsClient := &AFSClient{
		clientID:   clientID,
		cacheDir:   cacheDir,
		serverAddr: serverAddr,
		rpcClient:  client,
		cacheInfo:  make(map[string]*CachedFile),
	}

	log.Printf("AFS Client %s connected to %s", clientID, serverAddr)
	return afsClient, nil
}

func (c *AFSClient) Open(filename string, mode string) (*os.File, error) {
	log.Printf("Client %s opening file: %s", c.clientID, filename)

	// Check if file is in cache
	c.cacheMutex.RLock()
	cached, inCache := c.cacheInfo[filename]
	c.cacheMutex.RUnlock()

	if inCache {
		// Validate cache with server
		authReq := &utils.TestAuthRequest{
			ClientID: c.clientID,
			Filename: filename,
			Version:  cached.Version,
		}
		authResp := &utils.TestAuthResponse{}

		if err := c.rpcClient.Call("FileServer.TestAuth", authReq, authResp); err != nil {
			return nil, fmt.Errorf("TestAuth RPC failed: %v", err)
		}

		if authResp.Valid {
			log.Printf("Using cached version of %s", filename)
			return os.OpenFile(cached.LocalPath, os.O_RDWR, 0644)
		}

		log.Printf("Cache invalid for %s, fetching from server", filename)
	}

	// Fetch file from server
	if err := c.fetchFromServer(filename); err != nil {
		return nil, err
	}

	c.cacheMutex.RLock()
	localPath := c.cacheInfo[filename].LocalPath
	c.cacheMutex.RUnlock()

	return os.OpenFile(localPath, os.O_RDWR, 0644)
}

func (c *AFSClient) fetchFromServer(filename string) error {
	req := &utils.FetchFileRequest{
		ClientID: c.clientID,
		Filename: filename,
	}
	resp := &utils.FetchFileResponse{}

	if err := c.rpcClient.Call("FileServer.FetchFile", req, resp); err != nil {
		return fmt.Errorf("FetchFile RPC failed: %v", err)
	}

	if !resp.Success {
		return fmt.Errorf("fetch failed: %s", resp.Error)
	}

	// Write to local cache
	localPath := filepath.Join(c.cacheDir, filename)
	if err := os.WriteFile(localPath, resp.Content, 0644); err != nil {
		return fmt.Errorf("failed to write cache: %v", err)
	}

	// Update cache metadata
	c.cacheMutex.Lock()
	c.cacheInfo[filename] = &CachedFile{
		LocalPath: localPath,
		Version:   resp.Version,
		IsDirty:   false,
	}
	c.cacheMutex.Unlock()

	log.Printf("Cached %s (version %d, %d bytes)", filename, resp.Version, len(resp.Content))
	return nil
}

func (c *AFSClient) Read(filename string, offset int64, size int) ([]byte, error) {
	c.cacheMutex.RLock()
	cached, exists := c.cacheInfo[filename]
	c.cacheMutex.RUnlock()

	if !exists {
		return nil, fmt.Errorf("file not open: %s", filename)
	}

	file, err := os.Open(cached.LocalPath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	file.Seek(offset, 0)
	buffer := make([]byte, size)
	n, err := file.Read(buffer)
	return buffer[:n], err
}

func (c *AFSClient) Write(filename string, data []byte, offset int64) error {
	c.cacheMutex.RLock()
	cached, exists := c.cacheInfo[filename]
	c.cacheMutex.RUnlock()

	if !exists {
		return fmt.Errorf("file not open: %s", filename)
	}

	file, err := os.OpenFile(cached.LocalPath, os.O_RDWR, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	if _, err := file.Seek(offset, 0); err != nil {
		return err
	}

	if _, err := file.Write(data); err != nil {
		return err
	}

	// Mark as dirty
	c.cacheMutex.Lock()
	cached.IsDirty = true
	c.cacheMutex.Unlock()

	return nil
}

func (c *AFSClient) Close(filename string) error {
	c.cacheMutex.Lock()
	cached, exists := c.cacheInfo[filename]
	c.cacheMutex.Unlock()

	if !exists {
		return fmt.Errorf("file not open: %s", filename)
	}

	// If file was modified, flush to server
	if cached.IsDirty {
		if err := c.flushToServer(filename); err != nil {
			return err
		}

		c.cacheMutex.Lock()
		cached.IsDirty = false
		c.cacheMutex.Unlock()
	}

	log.Printf("Closed file: %s", filename)
	return nil
}

func (c *AFSClient) flushToServer(filename string) error {
	c.cacheMutex.RLock()
	cached := c.cacheInfo[filename]
	c.cacheMutex.RUnlock()

	content, err := os.ReadFile(cached.LocalPath)
	if err != nil {
		return fmt.Errorf("failed to read cache: %v", err)
	}

	req := &utils.StoreFileRequest{
		ClientID: c.clientID,
		Filename: filename,
		Content:  content,
	}
	resp := &utils.StoreFileResponse{}

	if err := c.rpcClient.Call("FileServer.StoreFile", req, resp); err != nil {
		return fmt.Errorf("StoreFile RPC failed: %v", err)
	}

	if !resp.Success {
		return fmt.Errorf("store failed: %s", resp.Error)
	}

	// Update version
	c.cacheMutex.Lock()
	cached.Version = resp.NewVersion
	c.cacheMutex.Unlock()

	log.Printf("Flushed %s to server (new version: %d)", filename, resp.NewVersion)
	return nil
}

func (c *AFSClient) Create(filename string) error {
	req := &utils.CreateFileRequest{
		ClientID: c.clientID,
		Filename: filename,
	}
	resp := &utils.CreateFileResponse{}

	if err := c.rpcClient.Call("FileServer.CreateFile", req, resp); err != nil {
		return fmt.Errorf("CreateFile RPC failed: %v", err)
	}

	if !resp.Success {
		return fmt.Errorf("create failed: %s", resp.Error)
	}

	log.Printf("Created file: %s", filename)
	return nil
}
