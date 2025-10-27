package afs

import (
	"fmt"
	"log"
	"net"
	"net/rpc"
	"os"
	"path/filepath"
	"sync"
	"time"

	utils "github.com/afs-simulator/pkg/utils"
)

type FileServer struct {
	inputDir  string
	outputDir string

	// File registry with metadata
	fileMutex sync.RWMutex
	files     map[string]*FileInfo

	// Track open files per client
	// openFiles sync.Map // map[clientID]map[filename]bool //gotta figure this out // clientid-filehandleid-isOpen
}

type FileInfo struct {
	Path         string
	Version      int64
	Size         int64
	LastModified time.Time
}

func NewFileServer(inputDir, outputDir string) (*FileServer, error) {
	// Validate directories exist
	if _, err := os.Stat(inputDir); os.IsNotExist(err) {
		return nil, fmt.Errorf("input directory does not exist: %s", inputDir)
	}
	if _, err := os.Stat(outputDir); os.IsNotExist(err) {
		if err := os.MkdirAll(outputDir, 0755); err != nil {
			return nil, fmt.Errorf("failed to create output directory: %v", err)
		}
	}

	fs := &FileServer{
		inputDir:  inputDir,
		outputDir: outputDir,
		files:     make(map[string]*FileInfo),
	}

	// Initialize file registry
	if err := fs.scanDirectory(inputDir); err != nil {
		return nil, err
	}

	return fs, nil
}

func (fs *FileServer) scanDirectory(dir string) error {
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
		go rpc.ServeConn(conn)
	}
}
