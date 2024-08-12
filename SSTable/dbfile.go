package sstable

import (
	"encoding/binary"
	"encoding/json"
	"log"
	"os"
)

func (table *SSTable) GetDbSize() int64 {
	info, err := os.Stat(table.file_path)
	if err != nil {
		log.Fatal(err)
	}
	return info.Size()
}

func (tree *TableTree) GetLevelSize(level int) int64 {
	size := int64(0)
	i := tree.levels[level]
	for ; i != nil; i = i.next {
		size += i.table.GetDbSize()
	}
	return size
}

func write_to_file(filepath string, data_area []byte, index_area []byte, meta MetaInfo) {
	f, err := os.OpenFile(filepath, os.O_WRONLY|os.O_CREATE, 0666)
	if err != nil {
		log.Fatal("error create file,", err)
	}
	_, err = f.Write(data_area)
	if err != nil {
		log.Fatal("error write file", err)
	}
	_, err = f.Write(index_area)
	if err != nil {
		log.Fatal("error write file", err)
	}
	_ = binary.Write(f, binary.LittleEndian, &meta.version)
	_ = binary.Write(f, binary.LittleEndian, &meta.data_start)
	_ = binary.Write(f, binary.LittleEndian, &meta.data_len)
	_ = binary.Write(f, binary.LittleEndian, &meta.index_start)
	_ = binary.Write(f, binary.LittleEndian, &meta.index_len)
	err = f.Sync()
	if err != nil {
		log.Fatalf("Failed to sync file: %v", err)
	}
}

func (table *SSTable) load_meta_info() {
	f := table.f
	_, err := f.Seek(0, 0)
	if err != nil {
		log.Println("error read file ", table.file_path)
		panic(err)
	}
	info, _ := f.Stat()
	size, kb := info.Size(), int64(METAINFOINT/8)

	_, err = f.Seek(size-kb*5, 0)
	if err != nil {
		log.Println("Error reading metadata ", table.file_path)
		panic(err)
	}
	_ = binary.Read(f, binary.LittleEndian, &table.table_meta_info.version)

	_, err = f.Seek(size-kb*4, 0)
	if err != nil {
		log.Println("Error reading metadata ", table.file_path)
		panic(err)
	}
	_ = binary.Read(f, binary.LittleEndian, &table.table_meta_info.data_start)

	_, err = f.Seek(size-kb*3, 0)
	if err != nil {
		log.Println("Error reading metadata ", table.file_path)
		panic(err)
	}
	_ = binary.Read(f, binary.LittleEndian, &table.table_meta_info.data_len)

	_, err = f.Seek(size-kb*2, 0)
	if err != nil {
		log.Println("Error reading metadata ", table.file_path)
		panic(err)
	}
	_ = binary.Read(f, binary.LittleEndian, &table.table_meta_info.index_start)

	_, err = f.Seek(size-kb, 0)
	if err != nil {
		log.Println("Error reading metadata ", table.file_path)
		panic(err)
	}
	_ = binary.Read(f, binary.LittleEndian, &table.table_meta_info.index_len)
}

func (table *SSTable) load_sparse_index() {
	bytes := make([]byte, table.table_meta_info.index_len)
	if _, err := table.f.Seek(table.table_meta_info.index_start, 0); err != nil {
		log.Println(" error open file ", table.file_path)
		panic(err)
	}
	if _, err := table.f.Read(bytes); err != nil {
		log.Println(" error open file ", table.file_path)
		panic(err)
	}

	table.sparse_index = make(map[string]Position)
	if err := json.Unmarshal(bytes, &table.sparse_index); err != nil {
		log.Println(" error open file ", table.file_path)
		panic(err)
	}
}
