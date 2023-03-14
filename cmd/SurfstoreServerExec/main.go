package main

import (
	ss "cse224/proj5/pkg/surfstore"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"os"
	"strconv"
	"strings"

	"google.golang.org/grpc"
)

// Usage String
const USAGE_STRING = "./run-server.sh -s <service_type> -p <port> -l -d (blockStoreAddr*)"

// Set of valid services
var SERVICE_TYPES = map[string]bool{"meta": true, "block": true, "both": true}

// Exit codes
const EX_USAGE int = 64

func main() {
	// Custom flag Usage message
	flag.Usage = func() {
		w := flag.CommandLine.Output()
		fmt.Fprintf(w, "Usage of %s:\n", USAGE_STRING)
		flag.VisitAll(func(f *flag.Flag) {
			fmt.Fprintf(w, "  -%s: %v\n", f.Name, f.Usage)
		})
		fmt.Fprintf(w, "  (blockStoreAddr*): BlockStore Address (include self if service type is both)\n")
	}

	// Parse command-line argument flags
	service := flag.String("s", "", "(required) Service Type of the Server: meta, block, both")
	port := flag.Int("p", 8080, "(default = 8080) Port to accept connections")
	localOnly := flag.Bool("l", false, "Only listen on localhost")
	debug := flag.Bool("d", false, "Output log statements")
	flag.Parse()

	// Use tail arguments to hold BlockStore address
	args := flag.Args()
	blockStoreAddrs := []string{}

	for _, blockstoreIP := range args {
		blockStoreAddrs = append(blockStoreAddrs, blockstoreIP)
	}
	fmt.Println(blockStoreAddrs)

	// Valid service type argument
	if _, ok := SERVICE_TYPES[strings.ToLower(*service)]; !ok {
		flag.Usage()
		os.Exit(EX_USAGE)
	}

	// Add localhost if necessary
	addr := ""
	if *localOnly {
		addr += "localhost"
	}
	addr += ":" + strconv.Itoa(*port)

	// Disable log outputs if debug flag is missing
	if !(*debug) {
		log.SetFlags(0)
		log.SetOutput(ioutil.Discard)
	}
	// logFile, err := os.Create("server.log")
	// if err != nil {
	// 	log.Fatal("Failed to open log file:", err)
	// }
	// defer logFile.Close()

	// log.SetOutput(logFile)
	log.Fatal(startServer(addr, strings.ToLower(*service), blockStoreAddrs))
}

func startServer(hostAddr string, serviceType string, blockStoreAddrs []string) error {
	fmt.Println("hostAddr: ", hostAddr, " serviceType: ", serviceType, " blockStoreAddr: ", blockStoreAddrs)
	//things to do:
	//1. do if else to choose what you're starting
	if serviceType == "meta" {
		// need to start a grpc server, register it for metastore with reference to the blockstore address
		lis, err := net.Listen("tcp", hostAddr)
		if err != nil {
			fmt.Printf("failed to listen: %v", err)
			return err
		}
		s := grpc.NewServer()
		ss.RegisterMetaStoreServer(s, ss.NewMetaStore(blockStoreAddrs))
		fmt.Printf("Meta server listening at %v registered with blockstore at %s", lis.Addr(), blockStoreAddrs)
		if err := s.Serve(lis); err != nil {
			fmt.Printf("failed to serve: %v", err)
			return err
		}
	} else if serviceType == "block" {
		// need to start a grpc server, register it for blockstore
		lis, err := net.Listen("tcp", hostAddr)
		if err != nil {
			fmt.Printf("failed to listen: %v", err)
			return err
		}
		s := grpc.NewServer()
		ss.RegisterBlockStoreServer(s, ss.NewBlockStore())
		fmt.Printf("Block server listening at %v", lis.Addr())
		if err := s.Serve(lis); err != nil {
			fmt.Printf("failed to serve: %v", err)
			return err
		}
	} else if serviceType == "both" {
		// need to start a grpc server, register it for metastore with reference to the blockstore address
		// need to start a grpc server, register it for blockstore
		lis, err := net.Listen("tcp", hostAddr)
		if err != nil {
			fmt.Printf("failed to listen: %v", err)
			return err
		}
		s := grpc.NewServer()
		ss.RegisterMetaStoreServer(s, ss.NewMetaStore(blockStoreAddrs))
		ss.RegisterBlockStoreServer(s, ss.NewBlockStore())
		fmt.Printf("Meta and Block server listening at %v", lis.Addr())
		if err := s.Serve(lis); err != nil {
			fmt.Printf("failed to serve: %v", err)
			return err
		}

	}
	return nil

}
