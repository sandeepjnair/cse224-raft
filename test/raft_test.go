package SurfTest

import (
	// "cse224/proj5/pkg/surfstore"
	"cse224/proj5/pkg/surfstore"
	"testing"
	"time"

	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

func TestRaftSetLeader(t *testing.T) {
	//Setup
	cfgPath := "./config_files/3nodes.txt"
	test := InitTest(cfgPath)
	defer EndTest(test)

	// TEST
	leaderIdx := 0
	test.Clients[leaderIdx].SetLeader(test.Context, &emptypb.Empty{})

	// heartbeat
	for _, server := range test.Clients {
		server.SendHeartbeat(test.Context, &emptypb.Empty{})
	}

	for idx, server := range test.Clients {
		// all should have the leaders term
		state, _ := server.GetInternalState(test.Context, &emptypb.Empty{})
		if state == nil {
			t.Fatalf("Could not get state")
		}
		if state.Term != int64(1) {
			t.Fatalf("Server %d should be in term %d", idx, 1)
		}
		if idx == leaderIdx {
			// server should be the leader
			if !state.IsLeader {
				t.Fatalf("Server %d should be the leader", idx)
			}
		} else {
			// server should not be the leader
			if state.IsLeader {
				t.Fatalf("Server %d should not be the leader", idx)
			}
		}
	}

	leaderIdx = 2
	test.Clients[leaderIdx].SetLeader(test.Context, &emptypb.Empty{})

	// heartbeat
	for _, server := range test.Clients {
		server.SendHeartbeat(test.Context, &emptypb.Empty{})
	}

	for idx, server := range test.Clients {
		// all should have the leaders term
		state, _ := server.GetInternalState(test.Context, &emptypb.Empty{})
		if state == nil {
			t.Fatalf("Could not get state")
		}
		if state.Term != int64(2) {
			t.Fatalf("Server should be in term %d", 2)
		}
		if idx == leaderIdx {
			// server should be the leader
			if !state.IsLeader {
				t.Fatalf("Server %d should be the leader", idx)
			}
		} else {
			// server should not be the leader
			if state.IsLeader {
				t.Fatalf("Server %d should not be the leader", idx)
			}
		}
	}
}

func TestRaftFollowersGetUpdates(t *testing.T) {
	t.Log("leader1 gets a request")
	//Setup
	cfgPath := "./config_files/3nodes.txt"
	test := InitTest(cfgPath)
	defer EndTest(test)

	// set node 0 to be the leader
	// TEST
	leaderIdx := 0
	test.Clients[leaderIdx].SetLeader(test.Context, &emptypb.Empty{})
	test.Clients[leaderIdx].SendHeartbeat(test.Context, &emptypb.Empty{})

	// here node 0 should be the leader, all other nodes should be followers
	// all logs and metastores should be empty
	// ^ TODO check
	for idx, server := range test.Clients {
		// all should have the leaders term
		state, _ := server.GetInternalState(test.Context, &emptypb.Empty{})
		if state == nil {
			t.Fatalf("Could not get state")
		}
		if state.Term != int64(1) {
			t.Fatalf("Server %d should be in term %d", idx, 1)
		}
		if idx == leaderIdx {
			// server should be the leader
			if !state.IsLeader {
				t.Fatalf("Server %d should be the leader", idx)
			}
		} else {
			// server should not be the leader
			if state.IsLeader {
				t.Fatalf("Server %d should not be the leader", idx)
			}
		}
	}

	// update a file on node 0
	filemeta := &surfstore.FileMetaData{
		Filename:      "testfile",
		Version:       1,
		BlockHashList: nil,
	}

	test.Clients[leaderIdx].UpdateFile(test.Context, filemeta)
	test.Clients[leaderIdx].SendHeartbeat(test.Context, &emptypb.Empty{})

	// one final call to sendheartbeat (from spec)
	test.Clients[leaderIdx].SendHeartbeat(test.Context, &emptypb.Empty{})

	// check that all the nodes are in the right state
	// leader and all followers should have the entry in the log
	// everything should be term 1
	// entry should be applied to all metastores
	// only node 0 should be leader
	goldenMeta := make(map[string]*surfstore.FileMetaData)
	goldenMeta[filemeta.Filename] = filemeta

	goldenLog := make([]*surfstore.UpdateOperation, 0)
	goldenLog = append(goldenLog, &surfstore.UpdateOperation{
		Term:         1,
		FileMetaData: filemeta,
	})

	term := int64(1)
	var leader bool
	for idx, server := range test.Clients {
		if idx == leaderIdx {
			leader = bool(true)
		} else {
			leader = bool(false)
		}

		_, err := CheckInternalState(&leader, &term, goldenLog, goldenMeta, server, test.Context)

		if err != nil {
			t.Fatalf("Error checking state for server %d: %s", idx, err.Error())
		}
	}
}

func TestRaftRecoverable(t *testing.T) {
	t.Log("leader1 gets a request while all other nodes are crashed. the crashed nodes recover.")
	//Setup
	cfgPath := "./config_files/3nodes.txt"
	test := InitTest(cfgPath)
	defer EndTest(test)

	// set node 0 to be the leader
	leaderIdx := 0
	test.Clients[leaderIdx].SetLeader(test.Context, &emptypb.Empty{})
	test.Clients[leaderIdx].SendHeartbeat(test.Context, &emptypb.Empty{})

	// crash the other nodes
	test.Clients[1].Crash(test.Context, &emptypb.Empty{})
	test.Clients[2].Crash(test.Context, &emptypb.Empty{})

	// create a request update a file on node 0
	filemeta := &surfstore.FileMetaData{
		Filename:      "testfile",
		Version:       1,
		BlockHashList: nil,
	}

	go test.Clients[leaderIdx].UpdateFile(test.Context, filemeta)
	go test.Clients[leaderIdx].SendHeartbeat(test.Context, &emptypb.Empty{})

	time.Sleep(200 * time.Millisecond)

	// the crashed nodes are restored
	test.Clients[1].Restore(test.Context, &emptypb.Empty{})
	test.Clients[2].Restore(test.Context, &emptypb.Empty{})

	// final heartbeat before collecting results
	test.Clients[leaderIdx].SendHeartbeat(test.Context, &emptypb.Empty{})

	// collect states
	goldenMeta := make(map[string]*surfstore.FileMetaData)
	goldenMeta[filemeta.Filename] = filemeta

	goldenLog := make([]*surfstore.UpdateOperation, 0)
	goldenLog = append(goldenLog, &surfstore.UpdateOperation{
		Term:         1,
		FileMetaData: filemeta,
	})

	term := int64(1)
	var leader bool
	for idx, server := range test.Clients {
		if idx == leaderIdx {
			leader = true
		} else {
			leader = false
		}

		_, err := CheckInternalState(&leader, &term, goldenLog, goldenMeta, server, test.Context)

		if err != nil {
			t.Fatalf("Error checking state for server %d: %s", idx, err.Error())
		}
	}

}

func TestRaftLogsCorrectlyOverwritten(t *testing.T) {
	t.Log("leader1 gets several requests while all other nodes are crashed. leader1 crashes. all other nodes are restored. leader2 gets a request. leader1 is restored.")

	//Setup
	cfgPath := "./config_files/3nodes.txt"
	test := InitTest(cfgPath)
	defer EndTest(test)

	// set node 0 to be the leader
	leaderIdx := 0
	test.Clients[leaderIdx].SetLeader(test.Context, &emptypb.Empty{})
	test.Clients[leaderIdx].SendHeartbeat(test.Context, &emptypb.Empty{})

	// crash all other nodes
	for idx, _ := range test.Clients {
		if idx != leaderIdx {
			test.Clients[idx].Crash(test.Context, &emptypb.Empty{})
		}
	}

	// create multiple requests to send to the leader
	filemeta1 := &surfstore.FileMetaData{
		Filename:      "testfile1",
		Version:       1,
		BlockHashList: nil,
	}

	filemeta2 := &surfstore.FileMetaData{
		Filename:      "testfile2",
		Version:       1,
		BlockHashList: nil,
	}

	// Send them to the leader
	go test.Clients[leaderIdx].UpdateFile(test.Context, filemeta1)
	go test.Clients[leaderIdx].SendHeartbeat(test.Context, &emptypb.Empty{})

	go test.Clients[leaderIdx].UpdateFile(test.Context, filemeta2)
	go test.Clients[leaderIdx].SendHeartbeat(test.Context, &emptypb.Empty{})

	time.Sleep(200 * time.Millisecond)

	// verify that the leader log has both the file entries
	goldenLog := []*surfstore.UpdateOperation{}
	goldenLog = append(goldenLog, &surfstore.UpdateOperation{FileMetaData: filemeta1, Term: 1})
	goldenLog = append(goldenLog, &surfstore.UpdateOperation{FileMetaData: filemeta2, Term: 1})

	goldenMeta := make(map[string]*surfstore.FileMetaData)

	for idx, server := range test.Clients {

		if idx == leaderIdx {
			// only the leader should have the two requests in its log
			// None should have been committed
			_, err := CheckInternalState(nil, nil, goldenLog, goldenMeta, server, test.Context)

			if err != nil {
				t.Fatalf("Error checking state for server %d: %s", idx, err.Error())
			}
		} else {
			// other nodes should have an empty log
			state, _ := test.Clients[idx].GetInternalState(test.Context, &emptypb.Empty{})
			if len(state.Log) != 0 {
				t.Fatalf("[%v] Peer log is not empty: %v\n", idx, state.Log)
			}
		}
	}

	// the leader crashes
	test.Clients[leaderIdx].Crash(test.Context, &emptypb.Empty{})

	// all others are restored
	for idx, _ := range test.Clients {
		if idx != leaderIdx {
			test.Clients[idx].Restore(test.Context, &emptypb.Empty{})
		}
	}

	// set the new leader
	leaderIdx = 1
	test.Clients[leaderIdx].SetLeader(test.Context, &emptypb.Empty{})
	test.Clients[leaderIdx].SendHeartbeat(test.Context, &emptypb.Empty{})

	// new request for the new leader
	filemeta3 := &surfstore.FileMetaData{
		Filename:      "testfile3",
		Version:       1,
		BlockHashList: nil,
	}

	// send the request to the new leader
	test.Clients[leaderIdx].UpdateFile(test.Context, filemeta3)
	test.Clients[leaderIdx].SendHeartbeat(test.Context, &emptypb.Empty{})

	// the original leader is restored
	test.Clients[0].Restore(test.Context, &emptypb.Empty{})

	// final heartbeat before collecting results
	test.Clients[leaderIdx].SendHeartbeat(test.Context, &emptypb.Empty{})

	// Check to verify that the client0 log has been overwritten
	goldenLog = []*surfstore.UpdateOperation{}
	goldenLog = append(goldenLog, &surfstore.UpdateOperation{FileMetaData: filemeta3, Term: 2})

	goldenMeta = make(map[string]*surfstore.FileMetaData)
	goldenMeta[filemeta3.Filename] = filemeta3

	var isLeader bool
	term := int64(2)
	for idx, server := range test.Clients {
		if idx == leaderIdx {
			isLeader = true
		} else {
			isLeader = false
		}

		// ensure that term = 2 for everyone & everyone has the same metastore and log.
		// ensure only node 1 is the leader
		_, err := CheckInternalState(&isLeader, &term, goldenLog, goldenMeta, server, test.Context)
		if err != nil {
			t.Fatalf("Error checking state for server %d: %s", idx, err.Error())
		}
	}

}
