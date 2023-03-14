package surfstore

import (
	context "context"
	"crypto/sha256"
	"encoding/hex"
	"errors"

	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

type BlockStore struct {
	BlockMap map[string]*Block
	UnimplementedBlockStoreServer
}

/* Hash Related */
func GetBlockHashBytesInBlockStore(blockData []byte) []byte {
	h := sha256.New()
	h.Write(blockData)
	return h.Sum(nil)
}

func GetBlockHashStringInBlockStore(blockData []byte) string {
	blockHash := GetBlockHashBytesInBlockStore(blockData)
	return hex.EncodeToString(blockHash)
}

// Return a list containing all blockHashes on this block server
func (bs *BlockStore) GetBlockHashes(ctx context.Context, _ *emptypb.Empty) (*BlockHashes, error) {
	hashes := []string{}
	for hash := range bs.BlockMap {
		hashes = append(hashes, hash)
	}
	return &BlockHashes{Hashes: hashes}, nil
}

func (bs *BlockStore) GetBlock(ctx context.Context, blockHash *BlockHash) (*Block, error) {
	//gets a block indexed by blockHash
	//if block doesn't exist, return nil and an error
	if block, ok := bs.BlockMap[blockHash.Hash]; ok {
		return block, nil
	} else {
		return nil, errors.New("hash key looking for not found")
	}

}

func (bs *BlockStore) PutBlock(ctx context.Context, block *Block) (*Success, error) {
	// takes a block and genertes a hash to index it by and stores it in the blockstore blockmap.
	// since there isn't a way to know what the chunk size is from the client we assume that only a single chunk
	// has been sent to the server to be put.
	blockHash := GetBlockHashStringInBlockStore(block.BlockData)
	// if the block already exists in the blockstore, return an error
	if _, ok := bs.BlockMap[blockHash]; ok {
		return &Success{Flag: false}, errors.New("data already present in block store what're you tryin to do bruh")
	} else { // if block doesn't already exist then put it in and return success
		bs.BlockMap[blockHash] = block
		return &Success{Flag: true}, nil
	}
}

// Given a list of hashes “in”, returns a list containing the
// subset of in that are stored in the key-value store
func (bs *BlockStore) HasBlocks(ctx context.Context, blockHashesIn *BlockHashes) (*BlockHashes, error) {
	// not doing any error handling here. See if that's necessary and

	HashesPresent := BlockHashes{}

	for _, hash := range blockHashesIn.GetHashes() {
		if _, ok := bs.BlockMap[hash]; ok {
			HashesPresent.Hashes = append(HashesPresent.Hashes, hash)
		}
	}
	return &HashesPresent, nil
}

// This line guarantees all method for BlockStore are implemented
var _ BlockStoreInterface = new(BlockStore)

func NewBlockStore() *BlockStore {
	return &BlockStore{
		BlockMap: map[string]*Block{},
	}
}
