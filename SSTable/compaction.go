package sstable

import (
	"log"
	"mylsmdb/config"
	"mylsmdb/kv"
	"mylsmdb/trees/redblacktree"
	"os"
	"time"
)

func (tree *TableTree) Check() {
	tree.major_compaction()
}

func (tree *TableTree) major_compaction() {
	conf := config.GetConfig()
	for i := range len(tree.levels) {
		table_size := int(tree.GetLevelSize(i) / 1000 / 1000)
		if _, count := tree.get_ic(i); count > conf.PartSize || table_size > levelMaxSize[i] {
			tree.major_compaction_level(i)
		}
	}
}

func (tree *TableTree) major_compaction_level(level int) {
	log.Println("Compressing layer ", level, " files")
	start := time.Now()
	defer func() {
		elapse := time.Since(start)
		log.Println("consumption of time : ", elapse)
	}()
	log.Printf("Compressing layer %d.db files\r\n", level)

	table_cache := make([]byte, levelMaxSize[level])

	MemTree := redblacktree.New[string, []byte]()
	tree.lock.Lock()

	for i := tree.levels[level]; i != nil; i = i.next {
		table := i.table
		if int64(len(table_cache)) < table.table_meta_info.data_len {
			table_cache = make([]byte, table.table_meta_info.data_len)
		}

		new_slice := table_cache[0:table.table_meta_info.data_len]
		if _, err := table.f.Seek(0, 0); err != nil {
			log.Println(" error open file ", table.file_path)
			panic(err)
		}
		if _, err := table.f.Read(new_slice); err != nil {
			log.Println(" error read file ", table.file_path)
			panic(err)
		}

		for k, pos := range table.sparse_index {
			if !pos.Del {
				value, err := kv.Decode[kv.Value](new_slice[pos.Start:(pos.Start + pos.Len)])
				if err != nil {
					log.Fatal(err)
				}
				MemTree.Put(k, value.Value)
			} else {
				MemTree.Remove(k)
			}
		}
	}
	tree.lock.Unlock()
	values := MemTree.ToValue()
	newl := min(level+1, 10)
	tree.createnode(values, newl)

	if level < 1 {
		oldnode := tree.levels[level]
		tree.levels[level] = nil
		tree.clear_level(oldnode)
	}
}

func (tree *TableTree) clear_level(node *tablenode) {
	tree.lock.Lock()
	defer tree.lock.Unlock()
	for ; node != nil; node = node.next {
		err := node.table.f.Close()
		if err != nil {
			log.Println(" error close file,", node.table.file_path)
			panic(err)
		}
		err = os.Remove(node.table.file_path)
		if err != nil {
			log.Println(" error delete file,", node.table.file_path)
			panic(err)
		}
		node.table.f = nil
		node.table = nil
	}
}
