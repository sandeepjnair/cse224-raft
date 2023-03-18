package surfstore

import (
	context "context"
	"fmt"
	"strings"
	"time"

	grpc "google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"
)

type RPCClient struct {
	MetaStoreAddrs []string
	BaseDir        string
	BlockSize      int
}

func (surfClient *RPCClient) GetBlockHashes(blockStoreAddr string, blockHashes *[]string) error {
	// call the block store server function to return a list of all block hashes in that server
	// connect to the server
	conn, err := grpc.Dial(blockStoreAddr, grpc.WithInsecure())
	if err != nil {
		return err
	}
	c := NewBlockStoreClient(conn)

	// perform the call
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	b, err := c.GetBlockHashes(ctx, &emptypb.Empty{})
	if err != nil {
		conn.Close()
		return err
	}
	*blockHashes = b.Hashes

	// close the connection
	return conn.Close()
}

func (surfClient *RPCClient) GetBlockStoreMap(blockHashesIn []string, blockStoreMap *map[string][]string) error {
	panic("todo")
}

func (surfClient *RPCClient) GetBlock(blockHash string, blockStoreAddr string, block *Block) error {
	// connect to the server
	conn, err := grpc.Dial(blockStoreAddr, grpc.WithInsecure())
	if err != nil {
		return err
	}
	c := NewBlockStoreClient(conn)

	// perform the call
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	b, err := c.GetBlock(ctx, &BlockHash{Hash: blockHash})
	if err != nil {
		conn.Close()
		return err
	}
	block.BlockData = b.BlockData
	block.BlockSize = b.BlockSize

	// close the connection
	return conn.Close()
}

func (surfClient *RPCClient) PutBlock(block *Block, blockStoreAddr string, succ *bool) error {
	// code to put a block to the blockstore server. send a block that needs to be put into the blockstore server

	// not sure if the implementation is fully correct or meets all edge cases

	// connect to the server
	conn, err := grpc.Dial(blockStoreAddr, grpc.WithInsecure())
	if err != nil {
		return err
	}
	c := NewBlockStoreClient(conn)

	// perform the call
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	success, err := c.PutBlock(ctx, block)
	if err != nil {
		conn.Close()
		return err
	}
	if !(*success).Flag {
		fmt.Println("PutBlock failed")
		return err
	}

	// close the connection
	return conn.Close()
}

func (surfClient *RPCClient) HasBlocks(blockHashesIn []string, blockStoreAddr string, blockHashesOut *[]string) error {
	//code to call the server implementation of HasBlocks

	// connect to the server
	conn, err := grpc.Dial(blockStoreAddr, grpc.WithInsecure())
	if err != nil {
		return err
	}
	c := NewBlockStoreClient(conn)

	// perform the call
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	blockHashes, err := c.HasBlocks(ctx, &BlockHashes{Hashes: blockHashesIn})
	blockHashesOut = &blockHashes.Hashes

	if err != nil {
		conn.Close()
		return err
	}
	// close the connection
	return conn.Close()
}

func (surfClient *RPCClient) GetFileInfoMap(serverFileInfoMap *map[string]*FileMetaData) error {
	//function to get the file info map from the metadata server

	// connect to the server
	// to_do need to connect to leader below instead of first address
	for i := 0; i < len(surfClient.MetaStoreAddrs); {
		conn, err := grpc.Dial(surfClient.MetaStoreAddrs[i], grpc.WithInsecure())
		if err != nil {
			return err
		}
		c := NewRaftSurfstoreClient(conn)

		// perform the call
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		remote_index, err := c.GetFileInfoMap(ctx, &emptypb.Empty{})
		if err != nil {
			// fmt.Println("debug err after getblockstoreaddrs call at client side:", err, "ERR_SERVER_CRASHED", ERR_SERVER_CRASHED.Error(), "bool check:", strings.Contains(err.Error(), "Server is crashed."))
			if strings.Contains(err.Error(), "Server is not the leader") || strings.Contains(err.Error(), "Server is crashed.") {
				i = (i + 1) % len(surfClient.MetaStoreAddrs)
				continue
			}
			conn.Close()
			return err
		}
		if remote_index.FileInfoMap == nil {
			remote_index.FileInfoMap = make(map[string]*FileMetaData)
		}
		*serverFileInfoMap = remote_index.FileInfoMap

		// close the connection
		return conn.Close()
	}

	return nil

}

func (surfClient *RPCClient) UpdateFile(fileMetaData *FileMetaData, latestVersion *int32) error {
	//function to call to update the remote index with the latest version of some file from client
	// it will put inside latestversion the version of the file that was updated, if it returns -1 then the
	// update wasn't successful and the client needs to handle it
	for i := 0; i < len(surfClient.MetaStoreAddrs); {
		// connect to the server
		conn, err := grpc.Dial(surfClient.MetaStoreAddrs[i], grpc.WithInsecure())
		if err != nil {
			return err
		}
		c := NewRaftSurfstoreClient(conn)

		// perform the call
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		version, err := c.UpdateFile(ctx, fileMetaData)
		if err != nil {
			// fmt.Println("debug err after getblockstoreaddrs call at client side:", err, "ERR_SERVER_CRASHED", ERR_SERVER_CRASHED.Error(), "bool check:", strings.Contains(err.Error(), "Server is crashed."))
			if strings.Contains(err.Error(), "Server is not the leader") || strings.Contains(err.Error(), "Server is crashed.") {
				i = (i + 1) % len(surfClient.MetaStoreAddrs)
				continue
			}
			conn.Close()
			return err
		}
		latestVersion = &version.Version

		// close the connection
		return conn.Close()
	}

	return nil
}

func (surfClient *RPCClient) GetBlockStoreAddrs(blockStoreAddrs *[]string) error {
	//function to call the server implementation of GetBlockStoreAddrs to get blockstore addresses from metastore

	for i := 0; i < len(surfClient.MetaStoreAddrs); {
		// connect to the server
		conn, err := grpc.Dial(surfClient.MetaStoreAddrs[i], grpc.WithInsecure())
		if err != nil {
			return err
		}
		c := NewRaftSurfstoreClient(conn)

		// perform the call
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		BlockStoreAddrs, err := c.GetBlockStoreAddrs(ctx, &emptypb.Empty{})

		if err != nil {
			fmt.Println("debug err after getblockstoreaddrs call at client side:", err)
			if strings.Contains(err.Error(), "Server is not the leader") || strings.Contains(err.Error(), "Server is crashed.") {
				i = (i + 1) % len(surfClient.MetaStoreAddrs)
				continue
			}
			conn.Close()
			return err
		}
		*blockStoreAddrs = BlockStoreAddrs.BlockStoreAddrs

		// close the connection
		return conn.Close()
	}

	return nil
}

// func (surfClient *RPCClient) GetBlockStoreAddr(blockStoreAddr *string) error {
// 	//function to call the server implementation of GetBlockStoreAddr to get blockstore address from metastore
// 	//you'll get the address in the blockStoreAddr pointer passed to the function

// 	// connect to the server
// 	conn, err := grpc.Dial(surfClient.MetaStoreAddr[0], grpc.WithInsecure())
// 	if err != nil {
// 		return err
// 	}
// 	c := NewRaftSurfstoreClient(conn)

// 	// perform the call
// 	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
// 	defer cancel()

// 	blockStoreAddrStruct, err := c.GetBlockStoreAddr(ctx, &emptypb.Empty{})
// 	*blockStoreAddr = (*blockStoreAddrStruct).Addr

// 	if err != nil {
// 		conn.Close()
// 		return err
// 	}
// 	// close the connection
// 	return conn.Close()

// }

// This line guarantees all method for RPCClient are implemented
var _ ClientInterface = new(RPCClient)

// Create an Surfstore RPC client
func NewSurfstoreRPCClient(addrs []string, baseDir string, blockSize int) RPCClient {
	return RPCClient{
		MetaStoreAddrs: addrs,
		BaseDir:        baseDir,
		BlockSize:      blockSize,
	}
}
