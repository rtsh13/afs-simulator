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
	randDelay := time.Duration(rand.Intn(1000)) * time.Millisecond

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

	fs.fileMutex.Lock()
	defer fs.fileMutex.Unlock()

	for _, entry := range entries {
		if !entry.IsDir() {
			// Get FileInfo to access Size() and ModTime()
			info, err := entry.Info()
			if err != nil {
				log.Printf("Warning: failed to get info for %s: %v", entry.Name(), err)
				continue
			}

			fullPath := filepath.Join(dir, entry.Name())
			fs.files[entry.Name()] = &FileInfo{
				Path:         fullPath,
				Version:      1,
				Size:         info.Size(),
				LastModified: info.ModTime(),
			}
		}
	}
	return nil
}

// RPC Methods
func (fs *FileServer) GetDirectories(req *utils.GetDirectoriesRequest, resp *utils.GetDirectoriesResponse) error {
	log.Printf("Client %s requesting directories", req.ClientID)
	resp.InputDir = fs.inputDir
	resp.OutputDir = fs.outputDir
	return nil
}

func (fs *FileServer) Open(req *utils.OpenRequest, resp *utils.OpenResponse) error {
	log.Printf("Client %s opening file: %s (mode: %s)", req.ClientID, req.Filename, req.Mode)

	fs.fileMutex.RLock()
	fileInfo, exists := fs.files[req.Filename]
	fs.fileMutex.RUnlock()

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

func (fs *FileServer) FetchFile(req *utils.FetchFileRequest, resp *utils.FetchFileResponse) error {
	log.Printf("Client %s fetching file: %s", req.ClientID, req.Filename)

	fs.fileMutex.RLock()
	fileInfo, exists := fs.files[req.Filename]
	fs.fileMutex.RUnlock()

	if !exists {
		resp.Success = false
		resp.Error = "file not found"
		return nil
	}

	// Read file content
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

func (fs *FileServer) TestAuth(req *utils.TestAuthRequest, resp *utils.TestAuthResponse) error {
	log.Printf("Client %s testing auth for file: %s (version: %d)",
		req.ClientID, req.Filename, req.Version)

	fs.fileMutex.RLock()
	fileInfo, exists := fs.files[req.Filename]
	fs.fileMutex.RUnlock()

	if !exists {
		resp.Valid = false
		return nil
	}

	resp.Valid = (fileInfo.Version == req.Version)
	resp.Version = fileInfo.Version
	return nil
}

func (fs *FileServer) StoreFile(req *utils.StoreFileRequest, resp *utils.StoreFileResponse) error {
	log.Printf("Client %s storing file: %s (%d bytes)",
		req.ClientID, req.Filename, len(req.Content))

	// Determine which directory to use
	var targetPath string
	if req.Filename == "primes.txt" {
		targetPath = filepath.Join(fs.outputDir, req.Filename)
	} else {
		targetPath = filepath.Join(fs.inputDir, req.Filename)
	}

	// Write file
	if err := os.WriteFile(targetPath, req.Content, 0644); err != nil {
		resp.Success = false
		resp.Error = fmt.Sprintf("failed to write file: %v", err)
		return nil
	}

	// Update metadata
	fs.fileMutex.Lock()
	if fileInfo, exists := fs.files[req.Filename]; exists {
		fileInfo.Version++
		fileInfo.Size = int64(len(req.Content))
		fileInfo.LastModified = time.Now()
		resp.NewVersion = fileInfo.Version
	} else {
		// New file
		fs.files[req.Filename] = &FileInfo{
			Path:         targetPath,
			Version:      1,
			Size:         int64(len(req.Content)),
			LastModified: time.Now(),
		}
		resp.NewVersion = 1
	}
	fs.fileMutex.Unlock()

	resp.Success = true
	return nil
}

func (fs *FileServer) CreateFile(req *utils.CreateFileRequest, resp *utils.CreateFileResponse) error {
	log.Printf("Client %s creating file: %s", req.ClientID, req.Filename)

	targetPath := filepath.Join(fs.outputDir, req.Filename)

	// Create empty file
	file, err := os.Create(targetPath)
	if err != nil {
		resp.Success = false
		resp.Error = fmt.Sprintf("failed to create file: %v", err)
		return nil
	}
	file.Close()

	// Add to registry
	fs.fileMutex.Lock()
	fs.files[req.Filename] = &FileInfo{
		Path:         targetPath,
		Version:      1,
		Size:         0,
		LastModified: time.Now(),
	}
	fs.fileMutex.Unlock()

	resp.Success = true
	return nil
}

func (fs *FileServer) Start(address string) error {
	rpc.Register(fs)

	listener, err := net.Listen("tcp", address)
	if err != nil {
		return fmt.Errorf("failed to listen: %v", err)
	}

	log.Printf("File server listening on %s", address)
	log.Printf("Input directory: %s", fs.inputDir)
	log.Printf("Output directory: %s", fs.outputDir)

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Printf("Accept error: %v", err)
			continue
		}
		go jsonrpc.ServeConn(conn)
	}
}
