package surfstore

import (
	"crypto/sha256"
	"encoding/hex"
	"sort"
)

type ConsistentHashRing struct {
	ServerMap map[string]string
}

func (c ConsistentHashRing) GetResponsibleServer(blockId string) string {
	// function to take in hash of a block of data as blockId and return the block store server
	// where you'll be able to find the data of the hash

	// find where each block belongs to
	// 1. sort hash values (key in hash ring)
	hashes := []string{}
	for h := range c.ServerMap {
		hashes = append(hashes, h)
	}
	sort.Strings(hashes)
	// 2. find the first server with larger hash value than blockId
	responsibleServer := ""
	for i := 0; i < len(hashes); i++ {
		if hashes[i] > blockId {
			responsibleServer = c.ServerMap[hashes[i]]
			break
		}
	}
	if responsibleServer == "" {
		responsibleServer = c.ServerMap[hashes[0]]
	}

	return responsibleServer
}

func (c ConsistentHashRing) Hash(addr string) string {
	h := sha256.New()
	h.Write([]byte(addr))
	return hex.EncodeToString(h.Sum(nil))

}

func NewConsistentHashRing(serverAddrs []string) *ConsistentHashRing {
	// hash servers on a hash ring
	consistentHashRing := make(map[string]string) // hash: serverName
	for _, serverName := range serverAddrs {
		serverHash := stringToHashString("blockstore" + serverName)
		consistentHashRing[serverHash] = serverName
	}
	return &ConsistentHashRing{ServerMap: consistentHashRing}
}

func stringToHashString(addr string) string {
	h := sha256.New()
	h.Write([]byte(addr))
	return hex.EncodeToString(h.Sum(nil))
}
