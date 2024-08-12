package sstable

import (
	"log"
	"mylsmdb/config"
	"mylsmdb/kv"
	"os"
	"sort"
	"strconv"
	"sync"
)

func (tree *TableTree) CreateNewTable(values []kv.Value) *SSTable {
	return tree.createnode(values, 0)
}

func (tree *TableTree) createnode(values []kv.Value, level int) *SSTable {
	key := make([]string, 0, len(values))
	position := make(map[string]Position)
	data_area := make([]byte, 0)
	for _, val := range values {
		data, err := kv.Encode(val)
		if err != nil {
			log.Print("fail to insert key : ", val.Key, err)
			continue
		}
		key = append(key, val.Key)
		position[val.Key] = Position{
			Start: int64(len(data_area)),
			Len:   int64(len(data)),
			Del:   val.Del,
		}
		data_area = append(data_area, data...)
	}
	sort.Strings(key)

	index_area, err := kv.Encode(position)
	if err != nil {
		log.Fatal("An SSTable file cannot be created,", err)
	}

	meta := MetaInfo{
		version:     0,
		data_start:  0,
		data_len:    int64(len(data_area)),
		index_start: int64(len(data_area)),
		index_len:   int64(len(index_area)),
	}

	table := &SSTable{
		table_meta_info: meta,
		sparse_index:    position,
		lock:            &sync.RWMutex{},
	}

	index := tree.insert(table, level)
	log.Printf("Create a new SSTable,level: %d ,index: %d\r\n", level, index)
	conf := config.GetConfig()
	file_path := conf.DataDir + "/" + strconv.Itoa(level) + "." + strconv.Itoa(index) + ".db"
	table.file_path = file_path

	write_to_file(file_path, data_area, index_area, meta)

	f, err := os.OpenFile(table.file_path, os.O_RDONLY, 0666)
	if err != nil {
		log.Println(" error open file ", table.file_path)
		panic(err)
	}
	table.f = f
	return table
}
