package lsm

import (
	sstable "mylsmdb/SSTable"
	"mylsmdb/skiplist"
	"mylsmdb/wal"
)

type DataBase struct {
	MemTree   *skiplist.SkipList[string, []byte]
	TableTree *sstable.TableTree
	Wal       *wal.Wal
}

var database *DataBase
