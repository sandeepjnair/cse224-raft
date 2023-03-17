package surfstore

import (
	context "context"
	"fmt"

	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

type MetaStore struct {
	FileMetaMap        map[string]*FileMetaData
	BlockStoreAddrs    []string
	ConsistentHashRing *ConsistentHashRing
	UnimplementedMetaStoreServer
}

func (m *MetaStore) GetFileInfoMap(ctx context.Context, _ *emptypb.Empty) (*FileInfoMap, error) {
	// right now this function just returns the FileMetaMap from MetaStrore Struct, doesn't look at whether
	// the file is enpty or not or do any other checks see if that needs to change
	fmt.Println("get file info map called from server, files in filematamap:")
	for key := range m.FileMetaMap {
		fmt.Println("filename: ", key)
	}
	return &FileInfoMap{FileInfoMap: m.FileMetaMap}, nil
}

func (m *MetaStore) UpdateFile(ctx context.Context, fileMetaData *FileMetaData) (*Version, error) {
	// going to change the FileMetaData in the FileMetaMap direclty if version is exactly one more than the
	// current version else returns -1 as version. if file is new then set version
	_, ok := m.FileMetaMap[fileMetaData.Filename]

	if !ok {
		// case when file is new we just add it to the fileMetaMap. Note here we're not checking if version is 1 or 0
		// we'll add it even if it comes with a verison of 2 as long as its new. This shouldn't be a problem but keep
		// in mind
		m.FileMetaMap[fileMetaData.Filename] = fileMetaData
		return &Version{Version: fileMetaData.Version}, nil
	} else if ok && fileMetaData.Version == (m.FileMetaMap[fileMetaData.Filename].Version+1) {
		// version is exactly one above everything is hunky dory
		m.FileMetaMap[fileMetaData.Filename] = fileMetaData
		return &Version{Version: fileMetaData.Version}, nil
	} else if ok && fileMetaData.BlockHashList[0] == "0" {
		// case in which server has a tombstone of the file. just take the version number from the client and update fileMetaMap
		if fileMetaData.Version > m.FileMetaMap[fileMetaData.Filename].Version {
			m.FileMetaMap[fileMetaData.Filename] = fileMetaData
			return &Version{Version: fileMetaData.Version}, nil
		} else {
			// case in which versions don't match for proper update.
			fmt.Println("version mismatch for update of file deleted in server")
			return &Version{Version: -1}, nil
		}
	} else {
		// case in which versions don't match for proper update. This will prompt client to make an update from
		// server
		return &Version{Version: -1}, nil
	}
}

// func (m *MetaStore) GetBlockStoreAddr(ctx context.Context, _ *emptypb.Empty) (*BlockStoreAddr, error) {
// 	// returns the blockstore address with nil error. This is just a getter function
// 	// fmt.Println("get block store addr called from server")

// 	return &BlockStoreAddr{Addr: m.BlockStoreAddr}, nil
// }

func (m *MetaStore) GetBlockStoreMap(ctx context.Context, blockHashesIn *BlockHashes) (*BlockStoreMap, error) {
	// returns a map of data blockhashes to blockstore addresses
	panic("to_do")
}

func (m *MetaStore) GetBlockStoreAddrs(ctx context.Context, _ *emptypb.Empty) (*BlockStoreAddrs, error) {
	return &BlockStoreAddrs{BlockStoreAddrs: m.BlockStoreAddrs}, nil
}

// This line guarantees all method for MetaStore are implemented
var _ MetaStoreInterface = new(MetaStore)

func NewMetaStore(blockStoreAddrs []string) *MetaStore {
	return &MetaStore{
		FileMetaMap:        map[string]*FileMetaData{},
		BlockStoreAddrs:    blockStoreAddrs,
		ConsistentHashRing: NewConsistentHashRing(blockStoreAddrs),
	}
}
