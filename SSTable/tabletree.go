package sstable

import (
	"fmt"
	"log"
	"mylsmdb/config"
	"mylsmdb/kv"
	"os"
	"path"
	"path/filepath"
	"sync"
	"time"
)

type TableTree struct {
	levels []*tablenode
	lock   *sync.RWMutex
}

type tablenode struct {
	index int
	table *SSTable
	next  *tablenode
}

var levelMaxSize []int

func (tree *TableTree) Init(dir string) {
	log.Println("loading SSTable...")
	start := time.Now()
	defer func() {
		elapse := time.Since(start)
		log.Println("consumption of time : ", elapse)
	}()

	conf := config.GetConfig()
	levelMaxSize = make([]int, 10)
	levelMaxSize[0] = conf.LevelSize
	for i := 1; i < 10; i++ {
		levelMaxSize[i] = levelMaxSize[i-1] * 10
	}

	tree.levels = make([]*tablenode, 10)
	tree.lock = &sync.RWMutex{}
	infos, err := os.ReadDir(dir)
	if err != nil {
		log.Println("Failed to read the database file")
		panic(err)
	}

	for _, info := range infos {
		if path.Ext(info.Name()) == ".db" {
			tree.load_dbfile(path.Join(dir, info.Name()))
		}
	}
}

func (tree *TableTree) load_dbfile(path string) {
	log.Println("Loading the ", path)
	start := time.Now()
	defer func() {
		elapse := time.Since(start)
		log.Println("consumption of time : ", elapse)
	}()

	level, index, err := get_level(filepath.Base(path))
	if err != nil {
		return
	}
	table := &SSTable{}
	table.Init(path)
	new_node := &tablenode{
		index: index,
		table: table,
		next:  nil,
	}

	i, pre_i := tree.levels[level], tree.levels[level]

	for ; i != nil && i.index < new_node.index; pre_i, i = i, i.next {
	}

	if i == pre_i {
		new_node.next = i
		tree.levels[level] = new_node
	} else {
		pre_i.next = new_node
		new_node.next = i
	}

}

func (tree *TableTree) insert(table *SSTable, level int) (index int) {
	tree.lock.Lock()
	defer tree.lock.Unlock()

	new_node := &tablenode{
		index: 0,
		table: table,
		next:  nil,
	}

	i, pre_i := tree.levels[level], tree.levels[level]
	for ; i != nil; pre_i, i = i, i.next {
	}
	if i == pre_i {
		tree.levels[level] = new_node
	} else {
		pre_i.next = new_node
		new_node.index = pre_i.index + 1
	}
	return new_node.index
}

func (tree *TableTree) Search(key string) (kv.Value, bool) {
	tree.lock.RLock()
	defer tree.lock.RUnlock()

	for _, node := range tree.levels {
		tables := make([]*SSTable, 0)
		for ; node != nil; node = node.next {
			tables = append(tables, node.table)
		}
		for i := len(tables) - 1; i >= 0; i-- {
			val, sr := tables[i].Search(key)
			if sr != kv.None {
				return val, sr == kv.Success
			}
		}
	}
	return kv.Value{}, false
}

// get max index and count of this level of the tree
func (tree *TableTree) get_ic(level int) (index int, count int) {
	node := tree.levels[level]
	index, count = 0, 0
	for ; node != nil; node = node.next {
		index = node.index
		count++
	}
	return index, count
}

func get_level(name string) (level int, index int, err error) {
	n, err := fmt.Sscanf(name, "%d.%d.db", &level, &index)
	if n != 2 || err != nil {
		return 0, 0, fmt.Errorf("incorrect data file name : %q", name)
	}
	return level, index, nil
}
