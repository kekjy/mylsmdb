package wal

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"log"
	"mylsmdb/kv"
	"mylsmdb/skiplist"
	"os"
	"path"
	"sync"
	"time"
)

type Wal struct {
	f    *os.File
	path string
	lock sync.Locker
}

func (w *Wal) Init(dir string) *skiplist.SkipList[string, []byte] {
	log.Println("Loading wal.log...")
	start := time.Now()
	defer func() {
		elapse := time.Since(start)
		log.Println("consumption of time : ", elapse)
	}()

	wal_path := path.Join(dir, "wal.log")
	f, err := os.OpenFile(wal_path, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		log.Println("The wal.log file cannot be created")
		panic(err)
	}
	w.f = f
	w.path = wal_path
	w.lock = &sync.Mutex{}
	return w.load_to_memory()
}

func (w *Wal) load_to_memory() *skiplist.SkipList[string, []byte] {
	w.lock.Lock()
	defer w.lock.Unlock()

	info, _ := os.Stat(w.path)
	size := info.Size()
	tree := skiplist.NewSkipList[string, []byte]()
	if size == 0 {
		return tree
	}

	_, err := w.f.Seek(0, 0)
	if err != nil {
		log.Println("Failed to open the wal.log")
		panic(err)
	}
	defer func(f *os.File, offset int64, whence int) {
		_, err := f.Seek(offset, whence)
		if err != nil {
			log.Println("Failed to open the wal.log")
			panic(err)
		}
	}(w.f, size-1, 0)

	data := make([]byte, size)
	_, err = w.f.Read(data)
	if err != nil {
		log.Println("Failed to open the wal.log")
		panic(err)
	}

	for i, len := int64(0), int64(0); i < size; {
		err := binary.Read(bytes.NewBuffer(data[i:(i+8)]), binary.LittleEndian, &len)
		if err != nil {
			log.Println("Failed to open the wal.log")
			panic(err)
		}

		i += 8
		var value kv.Value
		err = json.Unmarshal(data[i:(i+len)], &value)
		if err != nil {
			log.Println("Failed to open the wal.log")
			panic(err)
		}
		if value.Del {
			tree.Remove(value.Key)
		} else {
			tree.Put(value.Key, value.Value)
		}
		i += len
	}
	return tree
}

func (w *Wal) Write(value kv.Value) {
	w.lock.Lock()
	defer w.lock.Unlock()

	if value.Del {
		log.Println("wal.log:	delete ", value.Key)
	} else {
		log.Println("wal.log:	insert ", value.Key)
	}

	data, _ := json.Marshal(value)
	err := binary.Write(w.f, binary.LittleEndian, int64(len(data)))
	if err != nil {
		log.Println("Failed to write the wal.log")
		panic(err)
	}

	err = binary.Write(w.f, binary.LittleEndian, data)
	if err != nil {
		log.Println("Failed to write the wal.log")
		panic(err)
	}
}

func (w *Wal) Reset() {
	w.lock.Lock()
	defer w.lock.Unlock()

	log.Println("Resetting the wal.log file")

	err := w.f.Close()
	if err != nil {
		panic(err)
	}
	w.f = nil
	err = os.Remove(w.path)
	if err != nil {
		panic(err)
	}
	f, err := os.OpenFile(w.path, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		panic(err)
	}
	w.f = f
}
