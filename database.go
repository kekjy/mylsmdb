package lsm

import (
	sstable "mylsmdb/SSTable"
	"mylsmdb/trees/redblacktree"
	"mylsmdb/wal"
)

type DataBase struct {
	MemTree   *redblacktree.Tree[string, []byte]
	TableTree *sstable.TableTree
	Wal       *wal.Wal
}

var database *DataBase
