package utils

import "time"

// File metadata
type FileMetadata struct {
	Filename     string
	Size         int64
	Version      int64
	LastModified time.Time
}

// RPC Request/Response structures
type OpenRequest struct {
	ClientID string
	Filename string
	Mode     string // "r", "w", "rw"
}

type OpenResponse struct {
	Success  bool
	Metadata FileMetadata
	Error    string
}

type FetchFileRequest struct {
	ClientID string
	Filename string
}

type FetchFileResponse struct {
	Success bool
	Content []byte
	Version int64
	Error   string
}

type TestAuthRequest struct {
	ClientID string
	Filename string
	Version  int64
}

type TestAuthResponse struct {
	Valid   bool
	Version int64
}

type StoreFileRequest struct {
	ClientID string
	Filename string
	Content  []byte
}

type StoreFileResponse struct {
	Success    bool
	NewVersion int64
	Error      string
}

type CreateFileRequest struct {
	ClientID string
	Filename string
}

type CreateFileResponse struct {
	Success bool
	Error   string
}

type GetDirectoriesRequest struct {
	ClientID string
}

type GetDirectoriesResponse struct {
	InputDir  string
	OutputDir string
}

//PaxosServerInfo
//Check the datatypes here
type PaxosServerInfo struct {
	Address string
	NodeID float64
}

type ElectionInfo struct {
	ElectionNumber float64
	Coordinator *PaxosServerInfo
}


type LeaderRequest struct {
	Message string
	NodeIDs []float64
	ElectionNumber float64
}

//Would like to fix sending value as interface
type ProposalResponse struct {
	PromiseBool bool
	ProposalStamp float64	
	ExistingValue *interface{}
}

//Check over sending value as interface...
//Maybe needs to be turned into JSON? Or other
type AcceptanceRequest struct {
	ProposalStamp timestamp
	Content interface{}
}

type AcceptanceResponse struct {
	ProposalStamp timestamp
}