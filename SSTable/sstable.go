package sstable

import (
	"log"
	"mylsmdb/kv"
	"os"
	"sync"
)

type SSTable struct {
	f               *os.File
	file_path       string
	table_meta_info MetaInfo
	sparse_index    map[string]Position
	lock            sync.Locker
}

func (table *SSTable) Init(path string) {
	table.file_path = path
	table.lock = &sync.Mutex{}
	table.load_file_handle()
}

func (table *SSTable) load_file_handle() {
	if table.f == nil {
		f, err := os.OpenFile(table.file_path, os.O_RDONLY, 0666)
		if err != nil {
			log.Println(" error open file ", table.file_path)
			panic(err)
		}
		table.f = f
	}
	table.load_meta_info()
	table.load_sparse_index()
}

func (table *SSTable) Search(key string) (value kv.Value, result kv.SearchResult) {
	table.lock.Lock()
	defer table.lock.Unlock()
	/*l, r := 0, len(table.sort_index)
	for l + 1 < r {
		mid := (l + r) >> 1;
		if (table.sort_index[mid] <= key) {
			l = mid
		} else {
			r = mid
		}
	}*/
	pos, exist := table.sparse_index[key]
	if !exist {
		return kv.Value{}, kv.None
	}
	if pos.Del {
		return kv.Value{}, kv.Delete
	}
	bytes := make([]byte, pos.Len)
	if _, err := table.f.Seek(pos.Start, 0); err != nil {
		log.Println(err)
		return kv.Value{}, kv.None
	}
	if _, err := table.f.Read(bytes); err != nil {
		log.Println(err)
		return kv.Value{}, kv.None
	}

	value, err := kv.Decode[kv.Value](bytes)
	if err != nil {
		log.Println(err)
		return kv.Value{}, kv.None
	}
	return value, kv.Success
}
