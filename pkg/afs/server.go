package afs

import (
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/rpc"
	"net/rpc/jsonrpc"
	"os"
	"path/filepath"
	"sync"
	"time"

	utils "github.com/afs-simulator/pkg/utils"
)

const (
	writeOp = "write"
)

type FileInfo struct {
	Path         string
	Version      int64
	Size         int64
	LastModified time.Time
}

type ReplicaServer struct {
	id          string
	isPrimary   bool
	inputDir    string
	outputDir   string
	replicaAddr []string

	fileMutex sync.RWMutex
	files     map[string]*FileInfo

	replicationLog     []LogEntry
	replicationMutex   sync.Mutex
	logIndex           int64
	commitIndex        int64
	lastHeartbeat      time.Time
	heartbeatInterval  time.Duration
	electionTimeout    time.Duration
	replicaConnections map[string]*rpc.Client
}

type LogEntry struct {
	Index     int64
	Term      int64
	Operation string
	Filename  string
	Content   []byte
	Timestamp time.Time
}

type ReplicationRequest struct {
	Entry LogEntry
}

type ReplicationResponse struct {
	Success bool
	Index   int64
}

type HeartbeatRequest struct {
	LeaderID    string
	CommitIndex int64
	Timestamp   time.Time
}

type HeartbeatResponse struct {
	Success   bool
	ReplicaID string
}

// creates a new server that supports replication
func NewReplicaServer(id, inputDir, outputDir string, replicaAddrs []string) (*ReplicaServer, error) {
	if _, err := os.Stat(inputDir); os.IsNotExist(err) {
		return nil, fmt.Errorf("input directory does not exist: %s", inputDir)
	}

	if _, err := os.Stat(outputDir); os.IsNotExist(err) {
		if err := os.MkdirAll(outputDir, 0755); err != nil {
			return nil, fmt.Errorf("failed to create output directory: %v", err)
		}
	}

	// adding random delay to avoid leader election clash
	randDelay := time.Duration(rand.Intn(2000)) * time.Millisecond

	r := &ReplicaServer{
		id:                 id,
		isPrimary:          false,
		inputDir:           inputDir,
		outputDir:          outputDir,
		replicaAddr:        replicaAddrs,
		files:              make(map[string]*FileInfo),
		replicationLog:     make([]LogEntry, 0),
		logIndex:           0,
		commitIndex:        0,
		heartbeatInterval:  time.Second * 2,
		electionTimeout:    time.Second*5 + randDelay,
		lastHeartbeat:      time.Now(),
		replicaConnections: make(map[string]*rpc.Client),
	}

	if err := r.scanDirectory(inputDir); err != nil {
		return nil, err
	}

	// spawn a go routine to connect to replication servers
	go r.connectToReplicas()

	return r, nil
}

func (r *ReplicaServer) connectToReplicas() {
	for _, addr := range r.replicaAddr {
		go func(address string) {
			for {
				// a simple tcp conenct
				conn, err := net.Dial("tcp", address)
				if err != nil {
					time.Sleep(time.Second * 2)
					continue
				}

				client := jsonrpc.NewClient(conn)

				// each replication server gets a client created
				// client is indexed by its address
				r.replicationMutex.Lock()
				r.replicaConnections[address] = client
				r.replicationMutex.Unlock()

				log.Printf("Connected to replica at %s", address)
				return
			}
		}(addr)
	}
}

func (r *ReplicaServer) scanDirectory(dir string) error {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return err
	}

	r.fileMutex.Lock()
	defer r.fileMutex.Unlock()

	for _, entry := range entries {
		if !entry.IsDir() {
			info, err := entry.Info()
			if err != nil {
				log.Printf("Warning: failed to get info for %s: %v", entry.Name(), err)
				continue
			}

			fullPath := filepath.Join(dir, entry.Name())
			r.files[entry.Name()] = &FileInfo{
				Path:         fullPath,
				Version:      1,
				Size:         info.Size(),
				LastModified: info.ModTime(),
			}
		}
	}
	return nil
}

// once all servers are up based on user flag, the server becomes PRIMARY
func (r *ReplicaServer) BecomePrimary() {
	r.isPrimary = true
	log.Printf("Server %s became PRIMARY", r.id)
	go r.sendHeartbeats()
}

// upon PRIMARY failing, the replica is elected as PRIMARY
// throws heartbeat to rest replicas to assert dominance
func (r *ReplicaServer) electPrimary() {
	log.Printf("Server %s starting election process...", r.id)

	r.isPrimary = true
	r.lastHeartbeat = time.Now()
	log.Printf("Server %s became PRIMARY (via automated election)", r.id)

	go r.sendHeartbeats()
}

// monitors the last heartbeat of the replicas
// lets assume the PRIMARY died
// it would trigger that condition since the electred timeout will supersed lasthearbeat
func (r *ReplicaServer) monitorHeartbeat() {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for range ticker.C {
		if r.isPrimary {
			continue
		}

		if time.Since(r.lastHeartbeat) > r.electionTimeout {
			log.Printf("Server %s detected leader failure (no heartbeat for %v)", r.id, r.electionTimeout)
			r.electPrimary()
			return
		}
	}
}

// RPC method that just checks if the file exists
func (r *ReplicaServer) Open(req *utils.OpenRequest, resp *utils.OpenResponse) error {
	log.Printf("Client %s opening file: %s (mode: %s)", req.ClientID, req.Filename, req.Mode)

	r.fileMutex.RLock()
	fileInfo, exists := r.files[req.Filename]
	r.fileMutex.RUnlock()

	if !exists {
		resp.Success = false
		resp.Error = "file not found"
		return nil
	}

	resp.Success = true
	resp.Metadata = utils.FileMetadata{
		Filename:     req.Filename,
		Size:         fileInfo.Size,
		Version:      fileInfo.Version,
		LastModified: fileInfo.LastModified,
	}

	return nil
}

// only PRIMARY can send hearbeats
// helps PRIMARY keep replicas in consistent state
func (r *ReplicaServer) sendHeartbeats() {
	ticker := time.NewTicker(r.heartbeatInterval)
	defer ticker.Stop()

	for range ticker.C {
		if !r.isPrimary {
			return
		}

		r.replicationMutex.Lock()

		// loop over all listed replicas, and send heartbeats to record their health
		for addr, client := range r.replicaConnections {
			go func(address string, rpcClient *rpc.Client) {
				req := &HeartbeatRequest{
					// commit index always tells the current state of the replica
					// think of it always catching up with the log index
					CommitIndex: r.commitIndex,
					Timestamp:   time.Now(),
				}

				resp := &HeartbeatResponse{}

				// call the rpc to see liveliness
				if err := rpcClient.Call("ReplicaServer.Heartbeat", req, resp); err != nil {
					log.Printf("Heartbeat to %s failed: %v", address, err)
				}
			}(addr, client)
		}
		r.replicationMutex.Unlock()
	}
}

// this is a RPC for heartbeat to be only invoked via RPC call from PRIMARY
func (r *ReplicaServer) Heartbeat(req *HeartbeatRequest, resp *HeartbeatResponse) error {
	if r.isPrimary {
		log.Printf("Server %s stepping down after receiving heartbeat from Leader %s", r.id, req.LeaderID)
	}

	r.isPrimary = false
	r.lastHeartbeat = time.Now()
	r.commitIndex = req.CommitIndex

	resp.Success = true
	resp.ReplicaID = r.id
	return nil
}

// for write ops, we replicate to backups
// incase the PRIMARY dies before the replication is done, the system in in inconsistent state
// the next elected PRIMARY starts from a different log index as opposed to what we thought(maybe 5 but it starts at 4)
// this is acceptable because we aren't following WAL ideology before commiting to the file content on disk
func (r *ReplicaServer) replicateToBackups(entry LogEntry) error {
	if !r.isPrimary {
		return fmt.Errorf("only primary can replicate")
	}

	r.replicationMutex.Lock()
	// the log index is incremented before storing to the disk to capture the op
	r.logIndex++
	entry.Index = r.logIndex
	r.replicationLog = append(r.replicationLog, entry)
	r.replicationMutex.Unlock()

	var wg sync.WaitGroup
	successCount := 1
	var countMutex sync.Mutex

	for addr, client := range r.replicaConnections {
		wg.Add(1)
		go func(address string, rpcClient *rpc.Client) {
			defer wg.Done()

			resp := &ReplicationResponse{}

			if err := rpcClient.Call("ReplicaServer.Replicate",
				&ReplicationRequest{Entry: entry}, resp); err != nil {
				log.Printf("Replication to %s failed: %v", address, err)
			} else if resp.Success {
				countMutex.Lock()
				successCount++
				countMutex.Unlock()
			}
		}(addr, client)
	}

	wg.Wait()

	// we update the commit index to the current log index if the all replicas have synced
	majority := (len(r.replicaConnections) + 1) / 2
	if successCount >= majority {
		r.replicationMutex.Lock()
		r.commitIndex = entry.Index
		r.replicationMutex.Unlock()
		return nil
	}

	return fmt.Errorf("replication failed: only %d/%d replicas acknowledged",
		successCount, len(r.replicaConnections)+1)
}

// RPC to replicate the data
func (r *ReplicaServer) Replicate(req *ReplicationRequest, resp *ReplicationResponse) error {
	entry := req.Entry

	switch entry.Operation {
	case writeOp:
		targetPath := filepath.Join(r.outputDir, entry.Filename)
		if err := os.WriteFile(targetPath, entry.Content, 0644); err != nil {
			resp.Success = false
			return err
		}

		r.fileMutex.Lock()
		if fileInfo, exists := r.files[entry.Filename]; exists {
			fileInfo.Version++
			fileInfo.Size = int64(len(entry.Content))
			fileInfo.LastModified = time.Now()
		} else {
			r.files[entry.Filename] = &FileInfo{
				Path:         targetPath,
				Version:      1,
				Size:         int64(len(entry.Content)),
				LastModified: time.Now(),
			}
		}
		r.fileMutex.Unlock()
	}

	r.replicationMutex.Lock()
	r.replicationLog = append(r.replicationLog, entry)
	r.replicationMutex.Unlock()

	resp.Success = true
	resp.Index = entry.Index
	return nil
}

func (r *ReplicaServer) FetchFile(req *utils.FetchFileRequest, resp *utils.FetchFileResponse) error {
	log.Printf("Client %s fetching file: %s", req.ClientID, req.Filename)

	r.fileMutex.RLock()
	fileInfo, exists := r.files[req.Filename]
	r.fileMutex.RUnlock()

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

// store the updated file on disk
func (r *ReplicaServer) StoreFile(req *utils.StoreFileRequest, resp *utils.StoreFileResponse) error {
	if !r.isPrimary {
		resp.Success = false
		resp.Error = "not primary server"
		return nil
	}

	entry := LogEntry{
		Operation: writeOp,
		Filename:  req.Filename,
		Content:   req.Content,
		Timestamp: time.Now(),
	}

	if err := r.replicateToBackups(entry); err != nil {
		resp.Success = false
		resp.Error = fmt.Sprintf("replication failed: %v", err)
		return nil
	}

	targetPath := filepath.Join(r.outputDir, req.Filename)
	if err := os.WriteFile(targetPath, req.Content, 0644); err != nil {
		resp.Success = false
		resp.Error = fmt.Sprintf("failed to write file: %v", err)
		return nil
	}

	r.fileMutex.Lock()
	if fileInfo, exists := r.files[req.Filename]; exists {
		fileInfo.Version++
		fileInfo.Size = int64(len(req.Content))
		fileInfo.LastModified = time.Now()
		resp.NewVersion = fileInfo.Version
	} else {
		r.files[req.Filename] = &FileInfo{
			Path:         targetPath,
			Version:      1,
			Size:         int64(len(req.Content)),
			LastModified: time.Now(),
		}
		resp.NewVersion = 1
	}
	r.fileMutex.Unlock()

	resp.Success = true
	return nil
}

// RPC to test the validity of a dirty file
func (r *ReplicaServer) TestAuth(req *utils.TestAuthRequest, resp *utils.TestAuthResponse) error {
	r.fileMutex.RLock()
	fileInfo, exists := r.files[req.Filename]
	r.fileMutex.RUnlock()

	if !exists {
		resp.Valid = false
		return nil
	}

	resp.Valid = (fileInfo.Version == req.Version)
	resp.Version = fileInfo.Version
	return nil
}

// RPC to create a file
func (r *ReplicaServer) CreateFile(req *utils.CreateFileRequest, resp *utils.CreateFileResponse) error {
	log.Printf("Client %s creating file: %s", req.ClientID, req.Filename)

	if !r.isPrimary {
		resp.Success = false
		resp.Error = "not primary server"
		return nil
	}

	targetPath := filepath.Join(r.outputDir, req.Filename)

	file, err := os.Create(targetPath)
	if err != nil {
		resp.Success = false
		resp.Error = fmt.Sprintf("failed to create file: %v", err)
		return nil
	}
	file.Close()

	r.fileMutex.Lock()
	r.files[req.Filename] = &FileInfo{
		Path:         targetPath,
		Version:      1,
		Size:         0,
		LastModified: time.Now(),
	}
	r.fileMutex.Unlock()

	resp.Success = true
	return nil
}

func (r *ReplicaServer) GetStatus(req *struct{}, resp *map[string]interface{}) error {
	status := make(map[string]interface{})
	status["id"] = r.id
	status["is_primary"] = r.isPrimary
	status["log_index"] = r.logIndex
	status["commit_index"] = r.commitIndex
	status["files"] = len(r.files)
	status["replicas_connected"] = len(r.replicaConnections)

	*resp = status
	return nil
}

func (r *ReplicaServer) Start(address string) error {
	rpc.Register(r)

	listener, err := net.Listen("tcp", address)
	if err != nil {
		return fmt.Errorf("failed to listen: %v", err)
	}

	log.Printf("Replica server %s listening on %s", r.id, address)
	log.Printf("Input directory: %s", r.inputDir)
	log.Printf("Output directory: %s", r.outputDir)
	log.Printf("Primary: %v", r.isPrimary)

	go r.monitorHeartbeat()

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Printf("Accept error: %v", err)
			continue
		}
		go jsonrpc.ServeConn(conn)
	}
}

func (r *ReplicaServer) SaveSnapshot(filename string) error {
	r.fileMutex.RLock()
	r.replicationMutex.Lock()
	defer r.fileMutex.RUnlock()
	defer r.replicationMutex.Unlock()

	snapshot := map[string]interface{}{
		"id":           r.id,
		"is_primary":   r.isPrimary,
		"log_index":    r.logIndex,
		"commit_index": r.commitIndex,
		"files":        r.files,
		"log":          r.replicationLog,
		"timestamp":    time.Now(),
	}

	data, err := json.MarshalIndent(snapshot, "", "  ")
	if err != nil {
		return err
	}

	return os.WriteFile(filename, data, 0644)
}
