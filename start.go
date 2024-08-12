package lsm

import (
	"log"
	sstable "mylsmdb/SSTable"
	"mylsmdb/config"
	"mylsmdb/trees/redblacktree"
	"mylsmdb/wal"
	"os"
)

func Start(conf config.Config) {
	if database != nil {
		return
	}
	log.Println("Loading a Configuration File")
	config.Init(conf)
	log.Println("Initializing the database")
	init_database(conf.DataDir)
	log.Println("Performing background checks...")
	checkMemory()
	database.TableTree.Check()
	go Check()
}

func init_database(dir string) {
	database = &DataBase{
		MemTree:   &redblacktree.Tree[string, []byte]{},
		Wal:       &wal.Wal{},
		TableTree: &sstable.TableTree{},
	}

	if _, err := os.Stat(dir); err != nil {
		log.Printf("The %s directory does not exist. The directory is being created\r\n", dir)
		err := os.Mkdir(dir, 0666)
		if err != nil {
			log.Println("Failed to create the database directory")
			panic(err)
		}
	}

	MemTree := database.Wal.Init(dir)
	database.MemTree = MemTree
	log.Println("Loading database...")
	database.TableTree.Init(dir)
}
