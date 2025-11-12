package afs

import (
	"time"
)

type ReplicationRequest struct {
	Entry LogEntry
}

type ReplicationResponse struct {
	Success bool
	Index   int64
}

type HeartbeatRequest struct {
	LeaderID    int
	CommitIndex int64
	Timestamp   time.Time
}

type HeartbeatResponse struct {
	Success   bool
	ReplicaID int
}
