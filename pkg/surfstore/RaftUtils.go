package surfstore

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"sync"

	grpc "google.golang.org/grpc"
)

type RaftConfig struct {
	RaftAddrs  []string
	BlockAddrs []string
}

func LoadRaftConfigFile(filename string) (cfg RaftConfig) {
	configFD, e := os.Open(filename)
	if e != nil {
		log.Fatal("Error Open config file:", e)
	}
	defer configFD.Close()

	configReader := bufio.NewReader(configFD)
	decoder := json.NewDecoder(configReader)

	if err := decoder.Decode(&cfg); err == io.EOF {
		return
	} else if err != nil {
		log.Fatal(err)
	}
	return
}

func NewRaftServer(id int64, config RaftConfig) (*RaftSurfstore, error) {
	// TO_DO Any initialization you need here
	// initialize log with a nil value so that we can use index 1 to store the first log entry not sure
	// if any other initialization is needed
	isLeaderMutex := sync.RWMutex{}
	isCrashedMutex := sync.RWMutex{}

	// conns := make([]*grpc.ClientConn, 0)
	// clients := make([]RaftSurfstoreClient, 0)
	// for idx, addr := range config.RaftAddrs {
	// 	// no need to create a connection to itself
	// 	if int64(idx) == id {
	// 		continue
	// 	}
	// 	conn, err := grpc.Dial(addr, grpc.WithInsecure())
	// 	if err != nil {
	// 		log.Fatal("Error connecting to clients ", err)
	// 	}
	// 	client := NewRaftSurfstoreClient(conn)

	// 	conns = append(conns, conn)
	// 	clients = append(clients, client)
	// }

	log := make([]*UpdateOperation, 0)
	// log = append(log, &UpdateOperation{Term: 0, FileMetaData: nil})
	server := RaftSurfstore{
		serverId:       id,
		isLeader:       false,
		isLeaderMutex:  &isLeaderMutex,
		term:           0,
		metaStore:      NewMetaStore(config.BlockAddrs),
		RaftAddrs:      config.RaftAddrs,
		log:            log,
		isCrashed:      false,
		isCrashedMutex: &isCrashedMutex,
		commitIndex:    -1,
	}

	return &server, nil
}

// TODO Start up the Raft server and any services here
// implemented start up for raft server but not sure if there is any extra service that also needs to be started
func ServeRaftServer(server *RaftSurfstore) error {
	lis, err := net.Listen("tcp", server.RaftAddrs[server.serverId])
	if err != nil {
		fmt.Printf("failed to listen: %v", err)
		return err
	}
	s := grpc.NewServer()
	RegisterRaftSurfstoreServer(s, server)
	fmt.Printf("raft surfstore server listening at %v registered with blockstores at %s \n", lis.Addr(), server.metaStore.BlockStoreAddrs)
	if err := s.Serve(lis); err != nil {
		fmt.Printf("failed to serve: %v", err)
		return err
	}
	return nil
}
