package afs

import (
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/rpc"
	"net/rpc/jsonrpc"
	"os"
	"path/filepath"
	"sync"
	"time"

	utils "github.com/afs-simulator/pkg/utils"
)

// ReplicaServer represents a replicated file server
type ReplicaServer struct {
	id          string
	isPrimary   bool
	inputDir    string
	outputDir   string
	replicaAddr []string

	// File registry with metadata
	fileMutex sync.RWMutex
	files     map[string]*FileInfo

	// Replication state
	replicationLog     []LogEntry
	replicationMutex   sync.Mutex
	logIndex           int64
	commitIndex        int64
	lastHeartbeat      time.Time
	heartbeatInterval  time.Duration
	electionTimeout    time.Duration
	replicaConnections map[string]*rpc.Client
}

// LogEntry represents a replicated operation
type LogEntry struct {
	Index     int64
	Term      int64
	Operation string
	Filename  string
	Content   []byte
	Timestamp time.Time
}

// ReplicationRequest for primary-backup replication
type ReplicationRequest struct {
	Entry LogEntry
}

// ReplicationResponse for replication acknowledgment
type ReplicationResponse struct {
	Success bool
	Index   int64
}

// HeartbeatRequest for leader heartbeats
type HeartbeatRequest struct {
	LeaderID    string
	CommitIndex int64
	Timestamp   time.Time
}

// HeartbeatResponse for heartbeat acknowledgment
type HeartbeatResponse struct {
	Success   bool
	ReplicaID string
}

func NewReplicaServer(id, inputDir, outputDir string, replicaAddrs []string) (*ReplicaServer, error) {
	// Validate directories
	if _, err := os.Stat(inputDir); os.IsNotExist(err) {
		return nil, fmt.Errorf("input directory does not exist: %s", inputDir)
	}
	if _, err := os.Stat(outputDir); os.IsNotExist(err) {
		if err := os.MkdirAll(outputDir, 0755); err != nil {
			return nil, fmt.Errorf("failed to create output directory: %v", err)
		}
	}

	rs := &ReplicaServer{
		id:                 id,
		isPrimary:          false, // Initially not primary
		inputDir:           inputDir,
		outputDir:          outputDir,
		replicaAddr:        replicaAddrs,
		files:              make(map[string]*FileInfo),
		replicationLog:     make([]LogEntry, 0),
		logIndex:           0,
		commitIndex:        0,
		heartbeatInterval:  time.Second * 2,
		electionTimeout:    time.Second * 5,
		lastHeartbeat:      time.Now(),
		replicaConnections: make(map[string]*rpc.Client),
	}

	// Initialize file registry
	if err := rs.scanDirectory(inputDir); err != nil {
		return nil, err
	}

	// Connect to replicas
	go rs.connectToReplicas()

	return rs, nil
}

func (rs *ReplicaServer) connectToReplicas() {
	for _, addr := range rs.replicaAddr {
		go func(address string) {
			for {
				client, err := rpc.Dial("tcp", address)
				if err == nil {
					rs.replicationMutex.Lock()
					rs.replicaConnections[address] = client
					rs.replicationMutex.Unlock()
					log.Printf("Connected to replica at %s", address)
					return
				}
				time.Sleep(time.Second * 2)
			}
		}(addr)
	}
}

func (rs *ReplicaServer) scanDirectory(dir string) error {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return err
	}

	rs.fileMutex.Lock()
	defer rs.fileMutex.Unlock()

	for _, entry := range entries {
		if !entry.IsDir() {
			info, err := entry.Info()
			if err != nil {
				log.Printf("Warning: failed to get info for %s: %v", entry.Name(), err)
				continue
			}

			fullPath := filepath.Join(dir, entry.Name())
			rs.files[entry.Name()] = &FileInfo{
				Path:         fullPath,
				Version:      1,
				Size:         info.Size(),
				LastModified: info.ModTime(),
			}
		}
	}
	return nil
}

// Primary election (simplified - first server becomes primary)
func (rs *ReplicaServer) BecomePrimary() {
	rs.isPrimary = true
	log.Printf("Server %s became PRIMARY", rs.id)
	go rs.sendHeartbeats()
}

func (rs *ReplicaServer) sendHeartbeats() {
	ticker := time.NewTicker(rs.heartbeatInterval)
	defer ticker.Stop()

	for range ticker.C {
		if !rs.isPrimary {
			return
		}

		rs.replicationMutex.Lock()
		for addr, client := range rs.replicaConnections {
			go func(address string, rpcClient *rpc.Client) {
				req := &HeartbeatRequest{
					LeaderID:    rs.id,
					CommitIndex: rs.commitIndex,
					Timestamp:   time.Now(),
				}
				resp := &HeartbeatResponse{}

				if err := rpcClient.Call("ReplicaServer.Heartbeat", req, resp); err != nil {
					log.Printf("Heartbeat to %s failed: %v", address, err)
				}
			}(addr, client)
		}
		rs.replicationMutex.Unlock()
	}
}

// RPC: Heartbeat from primary
func (rs *ReplicaServer) Heartbeat(req *HeartbeatRequest, resp *HeartbeatResponse) error {
	rs.lastHeartbeat = time.Now()
	rs.commitIndex = req.CommitIndex

	resp.Success = true
	resp.ReplicaID = rs.id
	return nil
}

// Replicate operation to backups
func (rs *ReplicaServer) replicateToBackups(entry LogEntry) error {
	if !rs.isPrimary {
		return fmt.Errorf("only primary can replicate")
	}

	// Add to local log
	rs.replicationMutex.Lock()
	rs.logIndex++
	entry.Index = rs.logIndex
	rs.replicationLog = append(rs.replicationLog, entry)
	rs.replicationMutex.Unlock()

	// Send to replicas
	var wg sync.WaitGroup
	successCount := 1 // Count self
	var countMutex sync.Mutex

	for addr, client := range rs.replicaConnections {
		wg.Add(1)
		go func(address string, rpcClient *rpc.Client) {
			defer wg.Done()

			req := &ReplicationRequest{Entry: entry}
			resp := &ReplicationResponse{}

			if err := rpcClient.Call("ReplicaServer.Replicate", req, resp); err != nil {
				log.Printf("Replication to %s failed: %v", address, err)
			} else if resp.Success {
				countMutex.Lock()
				successCount++
				countMutex.Unlock()
			}
		}(addr, client)
	}

	wg.Wait()

	// Require majority (including self)
	majority := (len(rs.replicaConnections) + 1) / 2
	if successCount >= majority {
		rs.replicationMutex.Lock()
		rs.commitIndex = entry.Index
		rs.replicationMutex.Unlock()
		return nil
	}

	return fmt.Errorf("replication failed: only %d/%d replicas acknowledged",
		successCount, len(rs.replicaConnections)+1)
}

// RPC: Replicate from primary
func (rs *ReplicaServer) Replicate(req *ReplicationRequest, resp *ReplicationResponse) error {
	entry := req.Entry

	// Apply operation
	switch entry.Operation {
	case "write":
		targetPath := filepath.Join(rs.outputDir, entry.Filename)
		if err := os.WriteFile(targetPath, entry.Content, 0644); err != nil {
			resp.Success = false
			return err
		}

		// Update metadata
		rs.fileMutex.Lock()
		if fileInfo, exists := rs.files[entry.Filename]; exists {
			fileInfo.Version++
			fileInfo.Size = int64(len(entry.Content))
			fileInfo.LastModified = time.Now()
		} else {
			rs.files[entry.Filename] = &FileInfo{
				Path:         targetPath,
				Version:      1,
				Size:         int64(len(entry.Content)),
				LastModified: time.Now(),
			}
		}
		rs.fileMutex.Unlock()
	}

	// Add to log
	rs.replicationMutex.Lock()
	rs.replicationLog = append(rs.replicationLog, entry)
	rs.replicationMutex.Unlock()

	resp.Success = true
	resp.Index = entry.Index
	return nil
}

// RPC Methods (same as FileServer but with replication)
func (rs *ReplicaServer) FetchFile(req *utils.FetchFileRequest, resp *utils.FetchFileResponse) error {
	log.Printf("Client %s fetching file: %s", req.ClientID, req.Filename)

	rs.fileMutex.RLock()
	fileInfo, exists := rs.files[req.Filename]
	rs.fileMutex.RUnlock()

	if !exists {
		resp.Success = false
		resp.Error = "file not found"
		return nil
	}

	content, err := os.ReadFile(fileInfo.Path)
	if err != nil {
		resp.Success = false
		resp.Error = fmt.Sprintf("failed to read file: %v", err)
		return nil
	}

	resp.Success = true
	resp.Content = content
	resp.Version = fileInfo.Version
	return nil
}

func (rs *ReplicaServer) StoreFile(req *utils.StoreFileRequest, resp *utils.StoreFileResponse) error {
	log.Printf("Client %s storing file: %s (%d bytes)", req.ClientID, req.Filename, len(req.Content))

	if !rs.isPrimary {
		resp.Success = false
		resp.Error = "not primary server"
		return nil
	}

	// Create log entry
	entry := LogEntry{
		Operation: "write",
		Filename:  req.Filename,
		Content:   req.Content,
		Timestamp: time.Now(),
	}

	// Replicate to backups
	if err := rs.replicateToBackups(entry); err != nil {
		resp.Success = false
		resp.Error = fmt.Sprintf("replication failed: %v", err)
		return nil
	}

	// Update local state
	targetPath := filepath.Join(rs.outputDir, req.Filename)
	if err := os.WriteFile(targetPath, req.Content, 0644); err != nil {
		resp.Success = false
		resp.Error = fmt.Sprintf("failed to write file: %v", err)
		return nil
	}

	rs.fileMutex.Lock()
	if fileInfo, exists := rs.files[req.Filename]; exists {
		fileInfo.Version++
		fileInfo.Size = int64(len(req.Content))
		fileInfo.LastModified = time.Now()
		resp.NewVersion = fileInfo.Version
	} else {
		rs.files[req.Filename] = &FileInfo{
			Path:         targetPath,
			Version:      1,
			Size:         int64(len(req.Content)),
			LastModified: time.Now(),
		}
		resp.NewVersion = 1
	}
	rs.fileMutex.Unlock()

	resp.Success = true
	return nil
}

func (rs *ReplicaServer) TestAuth(req *utils.TestAuthRequest, resp *utils.TestAuthResponse) error {
	rs.fileMutex.RLock()
	fileInfo, exists := rs.files[req.Filename]
	rs.fileMutex.RUnlock()

	if !exists {
		resp.Valid = false
		return nil
	}

	resp.Valid = (fileInfo.Version == req.Version)
	resp.Version = fileInfo.Version
	return nil
}

func (rs *ReplicaServer) CreateFile(req *utils.CreateFileRequest, resp *utils.CreateFileResponse) error {
	log.Printf("Client %s creating file: %s", req.ClientID, req.Filename)

	if !rs.isPrimary {
		resp.Success = false
		resp.Error = "not primary server"
		return nil
	}

	targetPath := filepath.Join(rs.outputDir, req.Filename)

	file, err := os.Create(targetPath)
	if err != nil {
		resp.Success = false
		resp.Error = fmt.Sprintf("failed to create file: %v", err)
		return nil
	}
	file.Close()

	rs.fileMutex.Lock()
	rs.files[req.Filename] = &FileInfo{
		Path:         targetPath,
		Version:      1,
		Size:         0,
		LastModified: time.Now(),
	}
	rs.fileMutex.Unlock()

	resp.Success = true
	return nil
}

func (rs *ReplicaServer) GetStatus(req *struct{}, resp *map[string]interface{}) error {
	status := make(map[string]interface{})
	status["id"] = rs.id
	status["is_primary"] = rs.isPrimary
	status["log_index"] = rs.logIndex
	status["commit_index"] = rs.commitIndex
	status["files"] = len(rs.files)
	status["replicas_connected"] = len(rs.replicaConnections)

	*resp = status
	return nil
}

func (rs *ReplicaServer) Start(address string) error {
	rpc.Register(rs)

	listener, err := net.Listen("tcp", address)
	if err != nil {
		return fmt.Errorf("failed to listen: %v", err)
	}

	log.Printf("Replica server %s listening on %s", rs.id, address)
	log.Printf("Input directory: %s", rs.inputDir)
	log.Printf("Output directory: %s", rs.outputDir)
	log.Printf("Primary: %v", rs.isPrimary)

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Printf("Accept error: %v", err)
			continue
		}
		go jsonrpc.ServeConn(conn)
	}
}

// SaveSnapshot saves current state to disk
func (rs *ReplicaServer) SaveSnapshot(filename string) error {
	rs.fileMutex.RLock()
	rs.replicationMutex.Lock()
	defer rs.fileMutex.RUnlock()
	defer rs.replicationMutex.Unlock()

	snapshot := map[string]interface{}{
		"id":           rs.id,
		"is_primary":   rs.isPrimary,
		"log_index":    rs.logIndex,
		"commit_index": rs.commitIndex,
		"files":        rs.files,
		"log":          rs.replicationLog,
		"timestamp":    time.Now(),
	}

	data, err := json.MarshalIndent(snapshot, "", "  ")
	if err != nil {
		return err
	}

	return os.WriteFile(filename, data, 0644)
}
