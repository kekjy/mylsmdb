package lsm

import (
	"encoding/json"
	"log"
	"mylsmdb/kv"
)

func Get[T any](key string) (T, bool) {
	//debug
	//log.Print("Get ", key)
	value, found := database.MemTree.Get(key)
	if found {
		return getInstance[T](value)
	}

	if database.TableTree != nil {
		value, found := database.TableTree.Search(key)
		if found {
			return getInstance[T](value.Value)
		}
	}

	var nilt T
	return nilt, false
}

func Set[T any](key string, value T) bool {
	//debug
	//log.Print("Insert ", key, ",")
	val, err := kv.Encode(value)
	if err != nil {
		log.Println(err)
		return false
	}

	database.MemTree.Put(key, val)
	database.Wal.Write(kv.Value{
		Key:   key,
		Value: val,
		Del:   false,
	})
	return true
}

func DeleteAndGet[T any](key string) (T, bool) {
	//debug
	//log.Print("Delete ", key)
	value, found := database.MemTree.Remove(key)
	if found {
		// 写入 wal.log
		database.Wal.Write(kv.Value{
			Key:   key,
			Value: nil,
			Del:   true,
		})
		return getInstance[T](value)
	}
	var nilt T
	return nilt, false
}

func Delete[T any](key string) {
	//debug
	//log.Print("Delete ", key)
	_, found := database.MemTree.Remove(key)
	if found {
		database.Wal.Write(kv.Value{
			Key:   key,
			Value: nil,
			Del:   true,
		})
	}
}

func getInstance[T any](val []byte) (T, bool) {
	var v T
	err := json.Unmarshal(val, &v)
	if err != nil {
		log.Println(err)
	}
	return v, true
}
