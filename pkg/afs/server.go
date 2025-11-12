package afs

import (
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/rpc"
	"net/rpc/jsonrpc"
	"sync"
	"time"

	utils "github.com/afs-simulator/pkg/utils"
)

const (
	writeOp = "write"
)

type ReplicaServer struct {
	id          int
	isPrimary   bool
	replicaAddr []string
	afsServer   *AfsServer

	replicationLog     []LogEntry
	replicationMutex   sync.Mutex
	logIndex           int64
	commitIndex        int64
	lastHeartbeat      time.Time
	heartbeatInterval  time.Duration
	electionTimeout    time.Duration
	replicaConnections map[string]*rpc.Client
}

// creates a new server that supports replication
func NewReplicaServer(id int, workingDir string, replicaAddrs []string) (*ReplicaServer, error) {

	// adding random delay to avoid leader election clash
	randDelay := time.Duration(rand.Intn(2000)) * time.Millisecond

	r := &ReplicaServer{
		id:                 id,
		isPrimary:          false,
		replicaAddr:        replicaAddrs,
		replicationLog:     make([]LogEntry, 0),
		logIndex:           0,
		commitIndex:        0,
		heartbeatInterval:  time.Second * 2,
		electionTimeout:    time.Second*5 + randDelay,
		lastHeartbeat:      time.Now(),
		replicaConnections: make(map[string]*rpc.Client),
	}

	afs, err := NewAfsServer(workingDir)
	if err != nil {
		return nil, err
	}
	r.afsServer = afs

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

// once all servers are up based on user flag, the server becomes PRIMARY
func (r *ReplicaServer) BecomePrimary() {
	r.isPrimary = true
	log.Printf("Server %d became PRIMARY", r.id)
	go r.sendHeartbeats()
}

// upon PRIMARY failing, the replica is elected as PRIMARY
// throws heartbeat to rest replicas to assert dominance
func (r *ReplicaServer) electPrimary() {
	log.Printf("Server %d starting election process...", r.id)

	r.isPrimary = true
	r.lastHeartbeat = time.Now()
	log.Printf("Server %d became PRIMARY (via automated election)", r.id)

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
			log.Printf("Server %d detected leader failure (no heartbeat for %v)", r.id, r.electionTimeout)
			r.electPrimary()
			return
		}
	}
}

// RPC method that just checks if the file exists
func (r *ReplicaServer) Open(req *utils.OpenRequest, resp *utils.OpenResponse) error {
	log.Printf("Client %s opening file: %s (mode: %s)", req.ClientID, req.Filename, req.Mode)
	err := r.afsServer.Open(req, resp)
	if err != nil {
		return err
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
					LeaderID:    r.id,
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
	if r.id < req.LeaderID {
		if r.isPrimary {
			log.Printf("Server %d stepping down after receiving heartbeat from Leader %d", r.id, req.LeaderID)
		}

		r.isPrimary = false
		r.lastHeartbeat = time.Now()
		r.commitIndex = req.CommitIndex

		resp.Success = true
		resp.ReplicaID = r.id
		return nil
	}
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

	entry, err := r.afsServer.Replicate(req, resp)
	if err != nil {
		return err
	}

	r.replicationMutex.Lock()
	r.replicationLog = append(r.replicationLog, entry)
	r.replicationMutex.Unlock()
	return nil
}

func (r *ReplicaServer) FetchFile(req *utils.FetchFileRequest, resp *utils.FetchFileResponse) error {
	log.Printf("Client %s fetching file: %s", req.ClientID, req.Filename)

	err := r.afsServer.FetchFile(req, resp)

	if err != nil {
		return err
	}
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

	err := r.afsServer.StoreFile(req, resp)
	if err != nil {
		return err
	}
	return nil
}

// RPC to test the validity of a dirty file
func (r *ReplicaServer) TestAuth(req *utils.TestAuthRequest, resp *utils.TestAuthResponse) error {
	err := r.afsServer.TestAuth(req, resp)
	if err != nil {
		return err
	}
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

	err := r.afsServer.CreateFile(req, resp)
	if err != nil {
		return err
	}
	return nil
}

func (r *ReplicaServer) GetStatus(req *struct{}, resp *map[string]interface{}) error {
	status := make(map[string]interface{})
	status["id"] = r.id
	status["is_primary"] = r.isPrimary
	status["log_index"] = r.logIndex
	status["commit_index"] = r.commitIndex
	status["files"] = r.afsServer.GetFilesLen()
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
