package afs

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"

	utils "github.com/afs-simulator/pkg/utils"
)

type AfsServer struct {
	fileMutex  sync.RWMutex
	files      map[string]*FileInfo
	workingDir string
}

func NewAfsServer(workingDir string) (*AfsServer, error) {
	if _, err := os.Stat(workingDir); os.IsNotExist(err) {
		return nil, fmt.Errorf("input directory does not exist: %s", workingDir)
	}

	if _, err := os.Stat(workingDir); os.IsNotExist(err) {
		if err := os.MkdirAll(workingDir, 0755); err != nil {
			return nil, fmt.Errorf("failed to create output directory: %v", err)
		}
	}
	s := &AfsServer{
		files:      make(map[string]*FileInfo),
		workingDir: workingDir,
	}
	if err := s.scanDirectory(workingDir); err != nil {
		return nil, err
	}
	return s, nil
}

func (s *AfsServer) scanDirectory(dir string) error {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return err
	}

	s.fileMutex.Lock()
	defer s.fileMutex.Unlock()

	for _, entry := range entries {
		if !entry.IsDir() {
			info, err := entry.Info()
			if err != nil {
				log.Printf("Warning: failed to get info for %s: %v", entry.Name(), err)
				continue
			}

			fullPath := filepath.Join(dir, entry.Name())
			s.files[entry.Name()] = &FileInfo{
				Path:         fullPath,
				Version:      1,
				Size:         info.Size(),
				LastModified: info.ModTime(),
			}
		}
	}

	return nil
}

func (s *AfsServer) Open(req *utils.OpenRequest, resp *utils.OpenResponse) error {
	s.fileMutex.RLock()
	fileInfo, exist := s.files[req.Filename]
	s.fileMutex.RUnlock()

	if !exist {
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

func (s *AfsServer) FetchFile(req *utils.FetchFileRequest, resp *utils.FetchFileResponse) error {

	s.fileMutex.RLock()
	fileInfo, exists := s.files[req.Filename]
	s.fileMutex.RUnlock()

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

func (s *AfsServer) StoreFile(req *utils.StoreFileRequest, resp *utils.StoreFileResponse) error {

	targetPath := filepath.Join(s.workingDir, req.Filename)
	if err := os.WriteFile(targetPath, req.Content, 0644); err != nil {
		resp.Success = false
		resp.Error = fmt.Sprintf("failed to write file: %v", err)
		return nil
	}

	s.fileMutex.Lock()
	if fileInfo, exists := s.files[req.Filename]; exists {
		fileInfo.Version++
		fileInfo.Size = int64(len(req.Content))
		fileInfo.LastModified = time.Now()
		resp.NewVersion = fileInfo.Version
	} else {
		s.files[req.Filename] = &FileInfo{
			Path:         targetPath,
			Version:      1,
			Size:         int64(len(req.Content)),
			LastModified: time.Now(),
		}
		resp.NewVersion = 1
	}
	s.fileMutex.Unlock()

	resp.Success = true
	return nil
}

func (s *AfsServer) CreateFile(req *utils.CreateFileRequest, resp *utils.CreateFileResponse) error {

	targetPath := filepath.Join(s.workingDir, req.Filename)

	file, err := os.Create(targetPath)
	if err != nil {
		resp.Success = false
		resp.Error = fmt.Sprintf("failed to create file: %v", err)
		return nil
	}
	file.Close()

	s.fileMutex.Lock()
	s.files[req.Filename] = &FileInfo{
		Path:         targetPath,
		Version:      1,
		Size:         0,
		LastModified: time.Now(),
	}
	s.fileMutex.Unlock()

	resp.Success = true
	return nil
}

func (s *AfsServer) TestAuth(req *utils.TestAuthRequest, resp *utils.TestAuthResponse) error {
	s.fileMutex.RLock()
	fileInfo, exists := s.files[req.Filename]
	s.fileMutex.RUnlock()

	if !exists {
		resp.Valid = false
		return nil
	}

	resp.Valid = (fileInfo.Version == req.Version)
	resp.Version = fileInfo.Version
	return nil
}

func (s *AfsServer) Replicate(req *ReplicationRequest, resp *ReplicationResponse) (LogEntry, error) {
	entry := req.Entry

	switch entry.Operation {
	case writeOp:
		targetPath := filepath.Join(s.workingDir, entry.Filename)
		if err := os.WriteFile(targetPath, entry.Content, 0644); err != nil {
			resp.Success = false
			return entry, err
		}

		s.fileMutex.Lock()
		if fileInfo, exists := s.files[entry.Filename]; exists {
			fileInfo.Version++
			fileInfo.Size = int64(len(entry.Content))
			fileInfo.LastModified = time.Now()
		} else {
			s.files[entry.Filename] = &FileInfo{
				Path:         targetPath,
				Version:      1,
				Size:         int64(len(entry.Content)),
				LastModified: time.Now(),
			}
		}
		s.fileMutex.Unlock()
	}

	resp.Success = true
	resp.Index = entry.Index

	return entry, nil
}

func (s *AfsServer) GetFilesLen() int {
	return len(s.files)
}

func (s *AfsServer) getFiles() []FileInfo {
	files := []FileInfo{}
	for _, value := range s.files {
		files = append(files, *value)
	}
	return files
}

func (s *AfsServer) RLockFileMutex() {
	s.fileMutex.RLock()
}

func (s *AfsServer) RUnlockFileMutex() {
	s.fileMutex.Unlock()
}
