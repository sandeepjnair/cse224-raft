package surfstore

import (
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"log"
	"os"
	"path/filepath"

	_ "github.com/mattn/go-sqlite3"
)

/* Hash Related */
func GetBlockHashBytes(blockData []byte) []byte {
	h := sha256.New()
	h.Write(blockData)
	return h.Sum(nil)
}

func GetBlockHashString(blockData []byte) string {
	blockHash := GetBlockHashBytes(blockData)
	return hex.EncodeToString(blockHash)
}

/* File Path Related */
func ConcatPath(baseDir, fileDir string) string {
	return baseDir + "/" + fileDir
}

/*
	Writing Local Metadata File Related
*/

const createTable string = `create table if not exists indexes (
		fileName TEXT, 
		version INT,
		hashIndex INT,
		hashValue TEXT
	);`

const insertTuple string = "insert into indexes (fileName, version, hashIndex, hashValue) values (?, ?, ?, ?);"

// WriteMetaFile writes the file meta map back to local metadata file index.db
func WriteMetaFile(fileMetas map[string]*FileMetaData, baseDir string) error {
	//take local index and create a new index.db file from it

	// remove index.db file if it exists
	outputMetaPath := ConcatPath(baseDir, DEFAULT_META_FILENAME)
	if _, err := os.Stat(outputMetaPath); err == nil {
		e := os.Remove(outputMetaPath)
		if e != nil {
			log.Fatal("Error During Meta Write Back")
		}
	}
	db, err := sql.Open("sqlite3", outputMetaPath)
	if err != nil {
		log.Fatal("Error During Meta Write Back")
		return err
	}
	statement, err := db.Prepare(createTable)
	if err != nil {
		log.Fatal("Error During Meta Write Back")
		return err
	}
	statement.Exec()

	for fileName, fileMetaDate := range fileMetas {
		for hashIndex, hashValue := range fileMetaDate.BlockHashList {
			statement, err := db.Prepare(insertTuple)
			if err != nil {
				log.Fatal("Error During Meta Write Back")
			}
			statement.Exec(fileName, fileMetaDate.Version, hashIndex, hashValue)
		}
	}
	return nil
}

/*
Reading Local Metadata File Related
*/
const getDistinctFileName string = ``

const getTuplesByFileName string = ``

// LoadMetaFromMetaFile loads the local metadata file into a file meta map.
// The key is the file's name and the value is the file's metadata.
// You can use this function to load the index.db file in this project.
func LoadMetaFromMetaFile(baseDir string) (fileMetaMap map[string]*FileMetaData, e error) {
	// take index.db and load it into a map
	metaFilePath, _ := filepath.Abs(ConcatPath(baseDir, DEFAULT_META_FILENAME))
	fileMetaMap = make(map[string]*FileMetaData)
	metaFileStats, e := os.Stat(metaFilePath)
	if e != nil || metaFileStats.IsDir() {
		return fileMetaMap, nil
	}
	db, err := sql.Open("sqlite3", metaFilePath)
	if err != nil {
		log.Fatal("Error When Opening Meta")
	}
	getDistinctFileName := `SELECT DISTINCT fileName FROM indexes;`
	rows, err := db.Query(getDistinctFileName)

	for rows.Next() {
		var fileName string
		err = rows.Scan(&fileName)
		if err != nil {
			fmt.Println("Error When Reading Meta")
		}
		getTuplesByFileName := `SELECT version, hashIndex, hashValue FROM indexes WHERE fileName = ?;`
		tuples, err := db.Query(getTuplesByFileName, fileName)
		if err != nil {
			fmt.Println("Error When Reading from index.db inside surfstorehelper.go, loadMetaFromMetaFile")
		}
		var version int32
		var hashIndex int32
		var hashValue string
		var blockHashList []string
		for tuples.Next() {
			err = tuples.Scan(&version, &hashIndex, &hashValue)
			if err != nil {
				fmt.Println("Error When Reading Meta")
			}
			blockHashList = append(blockHashList, hashValue)
		}
		fileMetaMap[fileName] = &FileMetaData{Filename: fileName, Version: version, BlockHashList: blockHashList}
		fmt.Println("file name, version, going into localrealityindex", fileName, version)
	}

	return fileMetaMap, nil
}

/*
	Debugging Related
*/

// PrintMetaMap prints the contents of the metadata map.
// You might find this function useful for debugging.
func PrintMetaMap(metaMap map[string]*FileMetaData) {

	fmt.Println("--------BEGIN PRINT MAP--------")

	for _, filemeta := range metaMap {
		fmt.Println("\t", filemeta.Filename, filemeta.Version)
		for _, blockHash := range filemeta.BlockHashList {
			fmt.Println("\t", blockHash)
		}
	}

	fmt.Println("---------END PRINT MAP--------")

}
