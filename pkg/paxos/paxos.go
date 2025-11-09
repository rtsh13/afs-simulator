/*Overall todos:

1. Write ProposalResponse and AcceptResponse 
2. Big one is write the learner, ensure it returns at the end and incorporate with RPC intercept functions
3. Write forwarding behavior (i.e. if a client accesses the non coordinator server, forward to the coordinator)
4. Write heartbeats, timeouts, and RPC client redials
*/

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
	"math"

	utils "github.com/afs-simulator/pkg/utils"
)

type PaxosServer struct {
	fileServer FileServer,
	rpcClients []*rpc.Client,
	Servers []PaxosServerInfo,
	majorityRule float64,
	Election *ElectionInfo
}

func NewPaxosServer(inputDir, outputDir string, selfInfo PaxosServerInfo, servers []PaxosServerInfo, majorityRule *float64) (*PaxosServer, error) {
	//Default majority of 2/3
	if majorityRule == nil {
    	majorityRule = 2.0/3.0
  	}


	//Connect to the client for each provided server on initialization
	//TODO: allow the ability to add servers on the fly
	clients := []*rpc.Client{}
	for _, server := range servers {
		client, err := rpc.Dial("tcp", server.Address)
		if err != nil {
			fmt.Printf("Warning: server at %s not connected", server.Address)
			clients.append(nil)
			//return nil, fmt.Errorf("failed to connect to server: %v", err)
		}
		else {
			clients.append(client)
		}
	}

	//On initialization, no election has happened
	Election = ElectionInfo(0, nil);
	
	
	//Check error handling here
	ps := &PaxosServer{
		Info: selfInfo,
		rpcClients: clients,
		Servers: servers,
		fileServer:  NewFileServer(inputDir, outputDir)
		majorityRule: majorityRule,
	}

	return ps, nil
}


//Leader election
//Note: this method may mutate receivedNodeIDs
//Can be called with RPC or called by the follower when there's an issue
//TODO: fix variable defaults to work in method
func (ps *PaxosServer) RingElectLeader(Message string, NodeIDs []float64, ElectionNumber float64) error {
	if (Message == "") {
		Message = "election"
	}
	updateMessage := Message

	//Termination condition: if we're receiving the coordinator message but we already have the correct election number
	//Don't send to successor
	//TODO: check this condition for correctness 
	if (Message == "coordinator" && ElectionNumber == ps.Election.ElectionNumber) {
		return
	}

	latestElectionNum := math.Max(ElectionNumber, ps.Election.ElectionNumber)
	nodeIDsToSend := receivedNodeIDs

	//Search for (potential) coordinator and check for ring end simultaneously
	maxNodeID := -math.MaxFloat64
	for _, nodeID := range receivedNodeIds {
		if nodeID > maxNodeID {
			maxNodeID = nodeID
		}
		if nodeID == ps.Info.NodeID && Message == "election" {
			updateMessage = "coordinator"
		}
	}
	//If we are receiving coordinator update message, or the node sending the coordinator message, update host
	//election info
	if (Message == "coordinator "|| (Message == "election" && updateMessage == "coordinator")) {
		for _, server := range ps.Servers {
			if (server.NodeID == maxNodeID) {
				ps.ElectionInfo = ElectionInfo(latestElectionNum, server) 
				break
			}
		}
	}
	
	//If this is the first time we've received an election message, or we're sending out the first election message, append our node id
	if (updateMessage == "election") {
		append(nodeIDsToSend, ps.Info.NodeID) 
	}

	req := &utils.LeaderRequest{
		Message: updateMessage,
		NodeIDs: nodeIDsToSend,
		ElectionNumber: latestElectionNum
	}
	resp := &utils.LeaderResponse{}


	//Get successor to send to
	successorIndex := -1
	for i, server := range Servers {
		if server.nodeID == ps.Info.NodeID {
			if i < len(Servers)-1 {
				successorIndex = i+1
			}
			else {
				successorIndex = 0
			}
		}
	}

	//TODO: right now this could infinite loop if this server is the only one active
	while err := rpcClients[successorIndex].Call("PaxosServer.RingElectLeader", req, resp); err != nil {
		fmt.Printf("FetchFile RPC to  failed: %v", err)
		if successorIndex < len(Servers) - 1 {
			successorIndex = successorIndex + 1
		} 
		else {
			successorIndex = 0
		}
	}
}



//This is the key function to make Paxos work. Supports our proposals and acceptors
//This is half baked, it's not actually done yet
//TODO: Currently have generic input types, for request and response, might want a factory and/or type checking. Check over these inputs in general
func queryServers(rpcMethod string, request interface{}, response interface{}, servers []PaxosServerInfo, rpcClients []*rpc.Client, majorityRule float64) {
	//Servers
	calls := make([]*rpc.Call, len(servers))
	//Check errs implementation
	errs := make([]error, len(servers))

	//Note: we set default completions here to be the ceiling of 2/3 of servers
	//This is what provides Paxos guarantee of liveness? security? (one of those, I'm tired okay? Geez)
	//TODO: make comments more professional before turning in. Fucker
	completionWait := int(math.Ceil(float64(len(servers))*(majorityRule)))

	//Check over whether to use this
	//Edit this to be a buffered channel
	completionChan := make(chan struct{}, len(servers))

	//Note: right now sends to every server, including yourself
	for i := servers {
		//Use buffered channel here
		calls[i] := rpcClients[i].Go(rpcMethod, request, response, completionChan)
	}

	validResult :=  make([]*bool, len(servers))
	//Wait for buffered channel completion, handle here
	for i := 0; i < completionWait; i++ {
		<-completionChan // Receive signals from the completion channel
		for j := 0; j < len(servers); j++ {
			//If we got a valid result, that means it can be added to our majority of servers
			//Otherwise, we can't count it. 
			// TODO: Add early exit in the bad case
			if (calls[j] != nil && validResult[j] == nil) {
				validResult[j] = new(bool);
				validResult[j] = true;
			} else if (errs[j] != nil && validResult[j] == nil) {
				validResult[j] = = new(bool);
				validResult[j] = false;
				//Condition allows the process to fail
				if completionWait < len(servers) {
					completionWait = completionWait + 1
				}

			}
		}
	}

}

func (ps *PaxosServer) Propose(request interface{}, methodName string, timestamp *float64) {
	if (timestamp == nil) {
		timestamp = ps.Info.NodeID + ps.ElectionInfo.ElectionNumber
	}

	switch v := request.(type) {  // Type switch
	case OpenRequest:
		nil
	case FetchFileRequest:
		nil
	case TestAuthRequest:
		nil
	case StoreFileRequest:
		nil
	case CreateFileRequest:
		nil
	case GetDirectoriesRequest:
		nil
	default:
		return fmt.Errorf("Improper client request object", err)
	}

	//TODO: check these. A little iffy on request/response input here
	if (queryServers("PaxosServer.ProposalResponse", request, ProposalResponse(false, timestamp, nil), ps.Servers, ps.rpcClients, ps.majorityRule)) {
		Accept(request, methodName, timestamp)
	}
}

//Note: current implementation we'll send the whole request object across in the accept message
//Worth thinking through if this is a good idea
func (ps *PaxosServer) Accept(request interface{}, methodName string, timestamp float64) {
	switch v := request.(type) {  // Type switch
	case OpenRequest:
		nil
	case FetchFileRequest:
		nil
	case TestAuthRequest:
		nil
	case StoreFileRequest:
		nil
	case CreateFileRequest:
		nil
	case GetDirectoriesRequest:
		nil
	default:
		return fmt.Errorf("Improper client request object", err)
	}

	//TODO: update signature to work with queryServers
	queryServers("PaxosServer.AcceptanceResponse", ps.Servers, ps.rpcClients, ps.majorityRule)
}


//RPC intercepts


//Need to override all RPC methods to ensure that we don't learn before the propose-accept-learn process
//Guessing instead of fs, this should be PaxosServer
func (ps *PaxosServer) GetDirectories(req *utils.GetDirectoriesRequest, resp *utils.GetDirectoriesResponse) error {
	log.Printf("Server %s received proposal from client %s requesting directories", server, req.ClientID)

	//Need to handle response to this
	propose(req, "GetDirectories", timestamp)
	
	//TODO: populate todo with output from the learner
	return resp
}

//
func (ps *PaxosServer) Open(req *utils.OpenRequest, resp *utils.OpenResponse) error {
	log.Printf("Server %s received proposal from client %s opening file: %s (mode: %s)", server, req.ClientID, req.Filename, req.Mode)

	//Need to handle response to this
	propose(req, "Open", timestamp)

	//TODO: check if more needed response info
	resp.InputDir = fs.inputDir
	resp.OutputDir = fs.outputDir
	
	return resp
}

func (ps *PaxosServer) FetchFile(req *utils.FetchFileRequest, resp *utils.FetchFileResponse) error {
	log.Printf("Server %s received proposal from client %s fetching file: %s", server, req.ClientID, req.Filename)

	//Need to await response to this
	propose(req, "FetchFile", timestamp)

	//TODO: check if more needed response info
	resp.Success = true
	resp.Content = content
	resp.Version = fileInfo.Version

	return resp
}

func (ps *PaxosServer) TestAuth(req *utils.TestAuthRequest, resp *utils.TestAuthResponse) error {
	log.Printf("Server %s received proposal from client %s testing auth for file: %s (version: %d)",
		server, req.ClientID, req.Filename, req.Version)


	propose(req, "TestAuth", timestamp)

	//TODO: incorporate info from the learner into response here

	//if !exists {
	//	resp.Valid = false
	//	return nil
	//}

	//resp.Valid = (fileInfo.Version == req.Version)
	//resp.Version = fileInfo.Version
	
	return resp
}

func (ps *PaxosServer) StoreFile(req *utils.StoreFileRequest, resp *utils.StoreFileResponse) error {
	log.Printf("Server %s received proposal from client %s storing file: %s (%d bytes)",
		server, req.ClientID, req.Filename, len(req.Content))

	//TODO: incorporate info from learner into response here
	propose(req, "StoreFile", timestamp)

	//resp.Success = true
	return resp
}

func (ps *PaxosServer) CreateFile(req *utils.CreateFileRequest, resp *utils.CreateFileResponse) error {
	log.Printf("Server %s received proposal from client %s creating file: %s", server, req.ClientID, req.Filename)

	//TODO: incorporate info from learner into response here
	propose(req, "CreateFile", timestamp)
	//resp.Success = true
	return resp
}

//ls or lsdir
//

func (ps *PaxosServer) Start() error {
	rpc.Register(ps)
	rpc.RegisterName("FileServer", ps)

	listener, err := net.Listen("tcp", ps.Info.Address)
	if err != nil {
		return fmt.Errorf("failed to listen: %v", err)
	}

	log.Printf("Paxos server listening on %s", address)
	log.Printf("Input directory: %s", ps.fileServer.inputDir)
	log.Printf("Output directory: %s", ps.fileServer.outputDir)

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Printf("Accept error: %v", err)
			continue
		}
		go rpc.ServeConn(conn)
	}
}
