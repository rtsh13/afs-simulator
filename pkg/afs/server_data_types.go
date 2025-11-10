package afs

import (
	"time"
)

type LogEntry struct {
	Index     int64
	Term      int64
	Operation string
	Filename  string
	Content   []byte
	Timestamp time.Time
}

type FileInfo struct {
	Path         string
	Version      int64
	Size         int64
	LastModified time.Time
}
