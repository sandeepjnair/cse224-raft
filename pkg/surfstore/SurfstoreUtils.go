package surfstore

import (
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"reflect"
	"strings"
)

// type RPCClient struct {
// 	MetaStoreAddr string
// 	BaseDir       string
// 	BlockSize     int
// }

// func computeChunkHash(data []byte, h hash.Hash) []byte {
//     h.Reset()
//     h.Write(data)
//     return h.Sum(nil)
// }

// Implement the logic for a client syncing with the server here.
func ClientSync(client RPCClient) {
	//function to sync a client with the server. this involves
	// 1. scanning the client's base directory and get the hashlist of all files
	// 1.1. need to create index.db incase it doesn't exist in baseDir
	// 2. look at index.db in the client's base directory and look at if anything needs to change
	// 3. download FileInfoMap from the server
	// 4. compare the client's local and remote index.db
	//		4a. if the remote has a new file download and update local index
	//		4b. if local has new then upload and update server with new file info. If version is okay and update
	// 		succesful then update local.
	fmt.Println("ClientSync called with metaStoreAddr:", client.MetaStoreAddrs, " and baseDir:", client.BaseDir)
	//-------------------1, 1.1 and 2.-------------------
	index_present := getIndexPresence(client.BaseDir)
	localIndex := make(map[string]*FileMetaData)
	err := error(nil)
	if index_present {
		//fmt.Println("index.db is present")
		localIndex, err = LoadMetaFromMetaFile(client.BaseDir)
		if err != nil {
			//fmt.Println("Error loading index.db -- inside ClientSync")
		}
	} else {
		localIndex = make(map[string]*FileMetaData)
	}

	// in getLocalRealityIndex we go to every file, look at every chunk and put it into the appropriate blockstore
	// if its not already present in the localIndex map. We then update localrealityindex appropriately and then add
	// tombstone records for stuff present in localIndex but not in localrealityindex
	localRealityIndex := getLocalRealityIndex(client, localIndex)

	// print files in localRealityIndex
	// fmt.Println("files in localRealityIndex at the start of client sync:")
	// for filename := range localRealityIndex {
	// 	fmt.Println("filename:", filename)
	// }

	// err = WriteMetaFile(localRealityIndex, client.BaseDir) // DELETE THIS LINE LATER. its FOR TESTING!!!!!!!!!!!!!!!!!!!!!!!
	if !index_present {
		fmt.Println("index.db is not present, being created now")
		err := WriteMetaFile(localRealityIndex, client.BaseDir)
		if err != nil {
			fmt.Println("Error creating new index.db -- inside ClientSync")
		}
	}

	//-------------------3.-------------------
	//down load remoteindex from server
	remoteIndex := make(map[string]*FileMetaData)
	err = client.GetFileInfoMap(&remoteIndex)
	if err != nil {
		fmt.Println("Error getting remote index -- inside ClientSync", err)
	}
	fmt.Println("remoteIndex:", remoteIndex)
	// go through every file in remoteIndex
	//fmt.Println("files in remote index at the start of client sync:")
	for filename, remoteFileMetaData := range remoteIndex {
		//fmt.Println("filename:", filename)
		//iterate through remote index and compare with local reality index and make calls to download upload
		if localRealityFileMetaData, ok := localRealityIndex[filename]; ok {
			// file is present in localRealityIndex
			if localRealityFileMetaData.Version <= remoteFileMetaData.Version {
				// if local reality version is less than remote version need to update local file
				// if local realiity version is equal to remote version this might be because we've updated local reality
				// version over the local version due to a change locally but since there's a server update we overwrite
				// local update with server update info
				err = client.DownloadFile(filename, remoteFileMetaData.Version, remoteFileMetaData.BlockHashList, client.BaseDir)
				if err != nil {
					//fmt.Println("Error downloading file -- inside ClientSync")
				}
				localRealityIndex[filename] = &FileMetaData{Filename: filename, Version: remoteFileMetaData.Version, BlockHashList: remoteFileMetaData.BlockHashList}
			} else if localRealityFileMetaData.Version == 1+remoteFileMetaData.Version {
				// if local version is exactly one plus remote version need to update remote file
				// err = client.UploadFile(filename, localRealityFileMetaData.Version, client.BaseDir)
				if err != nil {
					//fmt.Println("Error uploading file -- inside ClientSync")
				}
				err = client.UpdateFile(localRealityFileMetaData, &localRealityFileMetaData.Version)
				if err == nil {
					remoteIndex[filename] = &FileMetaData{Filename: filename, Version: localRealityFileMetaData.Version, BlockHashList: localRealityFileMetaData.BlockHashList}
				}
			} else {
				// if version is equal than remote version do nothing
				// if version is greater than remote version by more than one then there is a version conflict
				if localRealityFileMetaData.Version > 1+remoteFileMetaData.Version {
					//fmt.Println("Version conflict -- inside ClientSync")
				}
			}
		} else {
			// file is not present in localRealityIndex but present in remoteIndex

			// if its not a tombstone record that localreality doesn't have then
			// download file and update localrealityIndex

			err = client.DownloadFile(filename, remoteFileMetaData.Version, remoteFileMetaData.BlockHashList, client.BaseDir)
			if err != nil {
				//fmt.Println("Error downloading file -- inside ClientSync")
			}
			localRealityIndex[filename] = &FileMetaData{Filename: filename, Version: remoteFileMetaData.Version, BlockHashList: remoteFileMetaData.BlockHashList}

			// if remote has a tombstone recoerd that local reality doesn't have then we don't need to do
			// anything as when remoteIndex is synced with localIndex it'll come over
			// else if val, ok := localIndex[remoteFileMetaData.Filename]; ok {
			// 	//it's possible remote has a file that localReality doesn't but its because it was there in localIndex but was
			// 	// deleted between syncs and hence didn't make it into localReality . In this case we need to update remote
			// 	//update remoteindex to have a tombstone record
			// 	//fmt.Println("updating remote with tomstone update")
			// 	err = client.UpdateFile(&FileMetaData{Filename: filename, Version: val.Version + 1, BlockHashList: []string{"0"}}, new(int32))
			// 	if err != nil {
			// 		//fmt.Println("Error updating remote index with tomstone update -- inside ClientSync")
			// 	}
			// }

		}
	}
	for filename, realityFileMetaData := range localRealityIndex {
		//right now localrealityindex has everything remoteindex has but not vice versa so we want
		//to add the files that are in localrealityindex but not in remoteindex
		if _, ok := remoteIndex[filename]; !ok {
			err = client.UpdateFile(realityFileMetaData, &realityFileMetaData.Version)
			if err == nil {
				remoteIndex[filename] = &FileMetaData{Filename: filename, Version: realityFileMetaData.Version, BlockHashList: realityFileMetaData.BlockHashList}
			} else {
				//fmt.Println("Error updating remote index -- inside ClientSync")
			}
		}
	}
	// at the end of clientsync local index should reflect remote index
	err = WriteMetaFile(remoteIndex, client.BaseDir)

}

func (client RPCClient) DownloadFile(filename string, version int32, hashlist []string, BaseDir string) error {
	//function to download a file from the server and put it into the base directory
	// 0. go through hashlist and figure out which blockstore you need to get the blocks from -- used consistent hashing
	// 1. download the blocks from the server
	// 2. create the file in the base directory
	blockStoreAddrs := new([]string)
	err := client.GetBlockStoreAddrs(blockStoreAddrs)
	ConsistentHashRing := NewConsistentHashRing(*blockStoreAddrs)
	//fmt.Println("downlaoding file from blockstore: ", filename, " version: ", version, " baseDir: ", BaseDir, " bStoreAddrs: ", *blockStoreAddrs)
	// 1. download the blocks from the server
	blockData := []byte{}
	bStoreAddr := ""
	for _, hash := range hashlist {
		block := new(Block)
		if hash != "0" {
			// used function to figure out what blockstore to get data using data hash
			bStoreAddr = ConsistentHashRing.GetResponsibleServer(hash)
			err := client.GetBlock(hash, bStoreAddr, block)
			if err != nil {
				//fmt.Println("Error getting block -- inside DownloadFile", err)
				return err
			}
			blockData = append(blockData, block.BlockData...)
		} else {
			//delete file from base directory
			filename = filepath.Join(BaseDir, filename)
			err = os.Remove(filename)
			if err != nil {
				//fmt.Println("Error deleting file -- inside DownloadFile", err)
				return err
			}
			//fmt.Println(" deleted file:", filename, "from base directory")
			return nil
		}
	}
	//fmt.Println("creating file in base directory")
	// 2. create the file in the base directory
	filename = filepath.Join(BaseDir, filename)
	err = ioutil.WriteFile(filename, blockData, 0644)
	if err != nil {
		//fmt.Println("Error writing file -- inside DownloadFile", err)
		return err
	}
	//fmt.Println("file created in base directory")
	return nil

}

func getIndexPresence(BaseDir string) bool {
	files, err := ioutil.ReadDir(BaseDir)
	if err != nil {
		//fmt.Println("Error reading directory -- inside getIndexPresence in ClientSync")
	}
	index_present := false
	for _, file := range files {
		// check if index.db exists
		if strings.Contains(file.Name(), "index.db") || strings.Contains(file.Name(), ",") || strings.Contains(file.Name(), "/") {
			index_present = true
			break
		}
	}
	return index_present
}

func getLocalRealityIndex(client RPCClient, localIndex map[string]*FileMetaData) map[string]*FileMetaData {
	//function gives the localRealityIndex and also sync all blocks whose hashes are newly generated to the blockstore
	var localRealityIndex map[string]*FileMetaData = make(map[string]*FileMetaData)

	files, err := ioutil.ReadDir(client.BaseDir)
	if err != nil {
		//fmt.Println("Error reading directory in getLocalRealityIndex -- inside ClientSync", err)
	}
	//fmt.Println("files in local reality index:")
	for _, file := range files {
		// .IsRegular makes sure that file is a file and not a directory or something
		if !file.Mode().IsRegular() || file.Name() == "index.db" || strings.Contains(file.Name(), ",") || strings.Contains(file.Name(), "/") {
			continue
		}
		//fmt.Println("filename:", file.Name())
		path := filepath.Join(client.BaseDir, file.Name())
		fileData, err := os.Open(path)
		if err != nil {
			//fmt.Println("Error opening file in getLocalRealityIndex -- inside ClientSync", err)

		}
		defer fileData.Close()

		buf := make([]byte, client.BlockSize)
		hashlist := []string{}
		for {
			n, err := fileData.Read(buf)
			if err == io.EOF {
				break
			}
			if err != nil {
				//fmt.Println("Error reading file in getLocalRealityIndex -- inside ClientSync", err)
			}

			chunkData := buf[:n]
			chunkHash := GetBlockHashString(chunkData)

			//------------ start of code to put block in blockstore if needed
			var blockStoreAddrs *[]string = new([]string)
			err = client.GetBlockStoreAddrs(blockStoreAddrs)
			ConsistentHashRing := NewConsistentHashRing(*blockStoreAddrs)
			if err != nil {
				//fmt.Println("Error getting blockstore address -- inside getLocalRealityIndex")
			}
			var succ *bool = new(bool)
			//sync the block to the blockstore if needed
			if indexFileMetaData, ok := localIndex[file.Name()]; ok && containsString(indexFileMetaData.BlockHashList, chunkHash) {
				//no need to put block again as its already present in blockstore. if chunkHash is not present
				//in localIndex then we need to put it in as done in else block
			} else {
				// figure out what blockstore you want to put the block in from blockstoreAddrs
				blockStoreAddr := ConsistentHashRing.GetResponsibleServer(chunkHash)
				err = client.PutBlock(&Block{BlockData: chunkData, BlockSize: int32(n)}, blockStoreAddr, succ)
				if err != nil {
					// //fmt.Println("Error syncing block -- inside getLocalRealityIndex", err)
					// //fmt.Println("fileName:", file.Name(), "chunkHash: ", chunkHash, "indexFileMetaData.BlockHashList: ", localIndex[file.Name()])
				}
			}
			hashlist = append(hashlist, string(chunkHash))
		}

		// checking if file is present in localIndex and if it is then check if the hashlist is same or not
		// and update the localRealityIndex accordingly
		if indexFileMetaData, ok := localIndex[file.Name()]; ok {
			if reflect.DeepEqual(hashlist, indexFileMetaData.BlockHashList) {
				localRealityIndex[file.Name()] = &FileMetaData{Filename: file.Name(), Version: indexFileMetaData.Version, BlockHashList: hashlist}
			} else {
				localRealityIndex[file.Name()] = &FileMetaData{Filename: file.Name(), Version: indexFileMetaData.Version + 1, BlockHashList: hashlist}
			}
		} else {
			localRealityIndex[file.Name()] = &FileMetaData{Filename: file.Name(), Version: 1, BlockHashList: hashlist}
		}
	}

	// setting tombstone record for files that are deleted by going through localIndex and adding tombstone for those not in
	// localRealityIndex
	for key := range localIndex {
		if _, ok := localRealityIndex[key]; !ok {
			if localIndex[key].BlockHashList[0] != "0" {

				localRealityIndex[key] = &FileMetaData{Filename: key, Version: localIndex[key].Version + 1, BlockHashList: []string{"0"}}
				err = client.UpdateFile(localRealityIndex[key], new(int32))
				if err != nil {
					//fmt.Println("Error updating remote index with tomstone update -- inside getLocalRealityIndex")
				}
			}

		}
	}

	return localRealityIndex
}

func containsString(slice []string, str string) bool {
	for _, s := range slice {
		if s == str {
			return true
		}
	}
	return false
}
