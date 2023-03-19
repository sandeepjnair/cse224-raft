package surfstore

import (
	context "context"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	grpc "google.golang.org/grpc"
	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

// TODO Add fields you need here
type RaftSurfstore struct {
	serverId      int64 // added this to store the server id of this raftsurfstore
	isLeader      bool
	isLeaderMutex *sync.RWMutex
	term          int64
	log           []*UpdateOperation
	RaftAddrs     []string // added this to store the port of the other servers
	metaStore     *MetaStore
	commitIndex   int64 // added as index of highest log entry known to be committed

	/*--------------- Chaos Monkey --------------*/
	isCrashed      bool
	isCrashedMutex *sync.RWMutex
	UnimplementedRaftSurfstoreServer
}

func (s *RaftSurfstore) GetFileInfoMap(ctx context.Context, empty *emptypb.Empty) (*FileInfoMap, error) {

	// return only after talking to a majority of servers
	// If the node is the leader, and if a majority of the nodes are working,
	// should return the correct answer; if a majority of the nodes are crashed,
	// should block until a majority recover.  If not the leader, should indicate
	// an error back to the client
	if !s.isLeader {
		return nil, ERR_NOT_LEADER
	} else if s.isCrashed {
		return nil, ERR_SERVER_CRASHED
	} else {
		concensus := false
		// keep trying till a majority of servers are alive
		for !concensus {
			success, err := s.SendHeartbeat(ctx, &emptypb.Empty{})
			if err != nil {
				if strings.Contains(err.Error(), "Server is not the leader") {
					return nil, ERR_NOT_LEADER
				} else if strings.Contains(err.Error(), "Server is crashed.") {
					return nil, ERR_SERVER_CRASHED
				} else {
					return nil, err
				}
			}
			concensus = success.Flag
		}

		return s.metaStore.GetFileInfoMap(ctx, empty)
	}

}

func (s *RaftSurfstore) GetBlockStoreMap(ctx context.Context, hashes *BlockHashes) (*BlockStoreMap, error) {

	// If the node is the leader, and if a majority of the nodes are working,
	// should return the correct answer; if a majority of the nodes are crashed,
	// should block until a majority recover.  If not the leader, should indicate
	// an error back to the client

	panic("to_do")
}

func (s *RaftSurfstore) GetBlockStoreAddrs(ctx context.Context, empty *emptypb.Empty) (*BlockStoreAddrs, error) {
	// directly call the metaStore's GetBlockStoreAddrs
	// If the node is the leader, and if a majority of the nodes are working,
	// should return the correct answer; if a majority of the nodes are crashed,
	// should block until a majority recover.  If not the leader, should indicate an
	// error back to the client
	fmt.Println("raftsurfstore.GetBlockStoreAddrs called on server ", s.serverId, " with isLeader: ", s.isLeader, " and isCrashed: ", s.isCrashed, "")
	if !s.isLeader {
		return nil, ERR_NOT_LEADER
	} else if s.isCrashed {
		return nil, ERR_SERVER_CRASHED
	} else {
		concensus := false
		// keep trying till a majority of servers are alive
		for !concensus {

			success, err := s.SendHeartbeat(ctx, &emptypb.Empty{})
			if err != nil {
				if strings.Contains(err.Error(), "Server is not the leader") {
					return nil, ERR_NOT_LEADER
				} else if strings.Contains(err.Error(), "Server is crashed.") {
					return nil, ERR_SERVER_CRASHED
				} else {
					return nil, err
				}
			}
			concensus = success.Flag
		}

		return s.metaStore.GetBlockStoreAddrs(ctx, empty)
	}
}

func (s *RaftSurfstore) UpdateFile(ctx context.Context, filemeta *FileMetaData) (*Version, error) {
	fmt.Println("raftsurfstore.UpdateFile called with filemeta: ", filemeta, " on server ", s.serverId, " with isLeader: ", s.isLeader)

	// if you're not a leader, you need to give back a ERR_NOT_LEADER error
	if !s.isLeader {
		return nil, ERR_NOT_LEADER
	} else if s.isCrashed {
		// if server is crashed, return ERR_SERVER_CRASHED
		return nil, ERR_SERVER_CRASHED
	} else {
		// add this file update to log after which loop through all the servers and try to append entries to them all
		s.log = append(s.log, &UpdateOperation{Term: s.term, FileMetaData: filemeta})
		// check if a majority of servers are alive and block until they are alive,
		// if they're alive update their logs with the new entry
		// successful only when both these happen and you get a majority of true
		success, err := s.SendHeartbeat(ctx, &emptypb.Empty{})
		if err != nil {
			if strings.Contains(err.Error(), "Server is not the leader") {
			} else {
				return nil, err
			}
		}
		// if majority of servers return true, then you can return the version
		// if not then you need to return -1 version and see how that is dealt with by client
		// result only returned after talking to a majority of servers
		if success.Flag {
			return s.metaStore.UpdateFile(ctx, s.log[len(s.log)-1].FileMetaData)
		} else {
			return &Version{Version: -1}, nil
		}
	}

}

// 1. Reply false if term < currentTerm (§5.1)
// 2. Reply false if log doesn’t contain an entry at prevLogIndex whose term
// matches prevLogTerm (§5.3)
// 3. If an existing entry conflicts with a new one (same index but different
// terms), delete the existing entry and all that follow it (§5.3)
// 4. Append any new entries not already in the log
// 5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index
// of last new entry)
func (s *RaftSurfstore) AppendEntries(ctx context.Context, input *AppendEntryInput) (*AppendEntryOutput, error) {
	// what does LeaderCommit mean and do? - latest commit index of the leader
	// does entries contain the entire log or just the new entries? - just the new entries
	fmt.Println("appendEntries called using server: ", s.serverId, "s.isLeader: ", s.isLeader, "s.isCrashed: ", s.isCrashed)

	if s.isCrashed {
		// if server is crashed, return ERR_SERVER_CRASHED
		fmt.Println("appendEntries returning false because server is crashed")
		return &AppendEntryOutput{Term: s.term, Success: false}, ERR_SERVER_CRASHED
	} else {
		// 1. Reply false if term < currentTerm (§5.1)
		if input.Term < s.term {
			// returning the current term and status of false
			fmt.Println("appendEntries returning false because input.Term < s.term")
			return &AppendEntryOutput{Term: s.term, Success: false}, nil
		}
		// if your term is less than the leader's term, you need to update your term
		if input.Term > s.term {
			s.term = input.Term
		}
		// if input.prevLogIndex is greater than the length of the log, then return false
		// need to get a longer inputEntries to append
		if input.PrevLogIndex > int64(len(s.log)-1) {
			fmt.Println("appendEntries returning false because input.PrevLogIndex > int64(len(s.log)-1)")
			return &AppendEntryOutput{Term: s.term, Success: false}, nil
		}

		// 2. Reply false if log doesn’t contain an entry at prevLogIndex whose term
		// matches prevLogTerm (§5.3)
		if s.log[input.PrevLogIndex].Term != input.PrevLogTerm {
			fmt.Println("appendEntries returning false because s.log[input.PrevLogIndex].Term != input.PrevLogTerm")
			return &AppendEntryOutput{Term: s.term, Success: false}, nil
		}
		// ideal case where the prevLogIndex and prevLogTerm match with last entry in the log
		if input.PrevLogIndex == int64(len(s.log)-1) && s.log[input.PrevLogIndex].Term == input.PrevLogTerm {
			// 4. Append any new entries not already in the log
			s.log = append(s.log, input.Entries...)
			// 5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index
			// of last new entry) and call update file on the metaStore
			s.commitIndex = input.LeaderCommit
			if s.log[len(s.log)-1].FileMetaData != nil {
				s.metaStore.UpdateFile(ctx, s.log[len(s.log)-1].FileMetaData)
			}
			return &AppendEntryOutput{ServerId: s.serverId, Term: s.term, Success: true, MatchedIndex: input.PrevLogIndex + 1}, nil
		}
		// 3. If an existing entry conflicts with a new one (same index but different
		// terms), delete the existing entry and all that follow it (§5.3)
		if int64(len(s.log)-1) > input.PrevLogIndex {
			// delete all entries after prevLogIndex
			s.log = s.log[:input.PrevLogIndex+1]
			// append everything in input.Entries from where the log matched
			// 4. Append any new entries not already in the log
			s.log = append(s.log, input.Entries...)
			// 5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index
			// of last new entry) and call update file on the metaStore
			s.commitIndex = input.LeaderCommit
			if s.log[len(s.log)-1].FileMetaData != nil {
				s.metaStore.UpdateFile(ctx, s.log[len(s.log)-1].FileMetaData)
			}
			return &AppendEntryOutput{ServerId: s.serverId, Term: s.term, Success: true, MatchedIndex: input.PrevLogIndex + 1}, nil
		}

		return nil, nil
	}
}

func (s *RaftSurfstore) SetLeader(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	// simply always setting the leader to true
	s.isLeaderMutex.Lock()
	s.isLeader = true
	s.isLeaderMutex.Unlock()

	// incrementing the term new leader is operating in
	s.term += 1
	fmt.Println("Leader is now: ", s.serverId, " in term: ", s.term, " with commitIndex: ", s.commitIndex)
	return &Success{Flag: true}, nil
}

func (s *RaftSurfstore) SendHeartbeat(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	// this is just an empty appendEntries call
	// 1. After every call to SetLeader the node that had SetLeader called will have SendHeartbeat called.
	// 2. After every UpdateFile call the node that had UpdateFile called will have SendHeartbeat called.
	// 3. After the test, the leader will have SendHeartbeat called one final time. Then all of the nodes should be ready for the internal state to be collected through GetInternalState.
	// 4. After every SyncClient operation

	// Sends a round of empty AppendEntries to all other nodes.
	// The leader will attempt to replicate logs to all other
	// nodes when this is called. It can be called even when
	// there are no entries to replicate. If a node is not in the leader state it should do nothing.
	fmt.Println("Sending heartbeat called from: ", s.serverId, "s.isLeader: ", s.isLeader)
	if !s.isLeader {
		return nil, ERR_NOT_LEADER
	} else if s.isCrashed {
		return nil, ERR_SERVER_CRASHED
	}
	// need to create a channel and call appendEntries goroutines on all the servers which return to the channel
	// if a majority responsd positively then return true else false.
	// if a server responds to appendEntries with a higher term, then you need to update your term and become a follower
	// if a server responds to appendEntries with a false, then you need to decrement the prevLogIndex and try again

	// create a channel to receive responses from the appendEntries calls
	resultChan := make(chan bool, len(s.RaftAddrs)) // creating a buffered channel of # of servers

	// loop thru servers and call function that will iteratively call appendEntries on the server till
	// its able to update the log on the server
	for idx, addr := range s.RaftAddrs {
		// if the server is the leader (one who calls heartbeat) then skip it
		if int64(idx) == (s.serverId) {
			continue
		}
		// call the function that will call appendEntries on the server
		go s.callAppendEntries(idx, addr, resultChan)
	}

	// now we need to wait for a majority of the servers to respond with true on result channel
	positive_responses := 1
	total_responses := 1
	for {
		select {
		case result := <-resultChan:
			total_responses += 1
			if result {
				positive_responses += 1
			}
		}
		if positive_responses > len(s.RaftAddrs)/2 {
			s.commitIndex = int64(len(s.log) - 1)
			if s.log[len(s.log)-1].FileMetaData != nil {
				s.metaStore.UpdateFile(ctx, s.log[len(s.log)-1].FileMetaData)
			}
			return &Success{Flag: true}, nil
		} else if total_responses == len(s.RaftAddrs) {
			fmt.Println("majority of servers have crashed")
			log.Fatal()
			return &Success{Flag: false}, nil
		}
	}

}

// ========== DO NOT MODIFY BELOW THIS LINE =====================================

func (s *RaftSurfstore) Crash(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	s.isCrashedMutex.Lock()
	s.isCrashed = true
	s.isCrashedMutex.Unlock()
	fmt.Println("just crashed server with id: ", s.serverId, "in term", s.term)
	return &Success{Flag: true}, nil
}

func (s *RaftSurfstore) Restore(ctx context.Context, _ *emptypb.Empty) (*Success, error) {
	s.isCrashedMutex.Lock()
	s.isCrashed = false
	s.isCrashedMutex.Unlock()
	fmt.Println("restored server with id: ", s.serverId, "in term", s.term)
	return &Success{Flag: true}, nil
}

func (s *RaftSurfstore) GetInternalState(ctx context.Context, empty *emptypb.Empty) (*RaftInternalState, error) {
	time.Sleep(2000 * time.Millisecond)
	fileInfoMap, _ := s.metaStore.GetFileInfoMap(ctx, empty)
	s.isLeaderMutex.RLock()
	state := &RaftInternalState{
		IsLeader: s.isLeader,
		Term:     s.term,
		Log:      s.log[1:],
		MetaMap:  fileInfoMap,
	}
	s.isLeaderMutex.RUnlock()
	fmt.Println("just got internal state from server with id: ", s.serverId, "in term", s.term, "with state::", state)
	return state, nil
}

var _ RaftSurfstoreInterface = new(RaftSurfstore)

// func MajorityAlive(s *RaftSurfstore) bool {
// 	// function returns true if majority if the raft nodes are alive
// 	// assuming whoever calls this function is alive
// 	alive := 1
// 	for idx, addr := range s.RaftAddrs {
// 		if int64(idx) == s.serverId {
// 			continue
// 		}
// 		conn, err := grpc.Dial(addr, grpc.WithInsecure())
// 		if err != nil {
// 			fmt.Println("couldn't connect to server with id", idx, "from server with id", s.serverId, "with error", err)
// 			continue
// 		}
// 		defer conn.Close()
// 		client := NewRaftSurfstoreClient(conn)
// 		_, err = client.UpdateFile(context.Background(), nil)
// 		if err == nil {
// 			alive++
// 		}
// 	}
// 	if alive > len(s.RaftAddrs)/2 {
// 		return true
// 	}
// 	return false
// }

func (s *RaftSurfstore) callAppendEntries(idx int, addr string, resultChan chan bool) {
	// idx: index of server which we want to call appendEntries on
	// addr: address of idx server
	// resultChan: channel on which we will send the result of appendEntries call

	// when you send a heartbeat you're calling appendentries with an empty entries array
	// you hope that append entries will verify that the last entry in your log is the same as the last entry in their log
	// ensuring that you're in sync if not you decrememt your prevLogIndex and try again until you fins a point you're in
	// sync and then you can replace the idx servers log with your (leader) log
	fmt.Println(" inside callappendEntries on server with id", idx, "from server with id", s.serverId, "with term", s.term, "and len(s.log) of ", len(s.log), "and commitIndex of ", s.commitIndex, "PrevLogIndex:", int64(len(s.log)-1), "PrevLogTerm:", s.log[len(s.log)-1].Term)
	input := &AppendEntryInput{
		Term:         s.term,
		PrevLogIndex: int64(len(s.log) - 1),
		PrevLogTerm:  s.log[len(s.log)-1].Term,
		Entries:      []*UpdateOperation{},
		LeaderCommit: s.commitIndex,
	}
	// append first element to input.Entries
	// input.Entries = append([]*UpdateOperation{s.log[len(s.log)-1]}, input.Entries...)
	// commented above line as you don't want to initially send anything with input.Entries, if leader and server are in sync there's nothing to append
	// inSync holds our understanding of whether the leader is inSync with idx server
	inSync := false
	// connect to the server
	conn, err := grpc.Dial(addr, grpc.WithInsecure())
	if err != nil {
		fmt.Println("couldn't connect to server with id", idx, "from server with id", s.serverId, "with error", err)
		// if the server can't be connected to, keep trying
	}
	defer conn.Close()

	client := NewRaftSurfstoreClient(conn)
	// go into a for loop and keep looping till leader and idx server are in sync, keep trying until they're back online
	for !inSync {

		// fmt.Println("trying to call appendEntries on server with id", idx, "using server with id", s.serverId, "with term", s.term, "and len(s.log) of ", len(s.log), "and commitIndex of ", s.commitIndex, "PrevLogIndex:", input.PrevLogIndex, "PrevLogTerm:", input.PrevLogTerm)
		// call appendEntries on the server
		output, err := client.AppendEntries(context.Background(), input)
		if err != nil {
			if strings.Contains(err.Error(), "Server is crashed.") {
				// fmt.Println("server with id", idx, " called from server with id", s.serverId, "is crashed")
				// resultChan <- false
				// return
				continue
			} else {
				fmt.Println("couldn't call appendEntries on server with id", idx,
					"from server with id", s.serverId, "with error", err, "with addr", addr)
				resultChan <- false
				return
			}
		}

		// if the server responds with a higher term, then update the term and become a follower
		if output.Term > s.term {
			s.term = output.Term
			s.isLeaderMutex.Lock()
			s.isLeader = false
			s.isLeaderMutex.Unlock()
			resultChan <- false
			return
		}

		// if the server responds with a false, then decrement the prevLogIndex and try again
		if !output.Success {
			input.PrevLogIndex -= 1
			input.PrevLogTerm = s.log[input.PrevLogIndex].Term
			// append first element to input.Entries
			input.Entries = append([]*UpdateOperation{s.log[input.PrevLogIndex+1]}, input.Entries...)
			continue
		}

		// if the server responds with a true, then we are in sync
		if output.Success {
			inSync = true
			resultChan <- true
		}
	}

}
