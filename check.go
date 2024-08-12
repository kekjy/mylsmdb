package lsm

import (
	"log"
	"mylsmdb/config"
	"mylsmdb/trees/redblacktree"
	"time"
)

func Check() {
	conf := config.GetConfig()
	ticker := time.Tick(time.Duration(conf.CompressionInterval) * time.Second)
	for range ticker {
		log.Println("Performing background checks...")
		// 检查内存
		checkMemory()
		// 检查压缩数据库文件
		database.TableTree.Check()
	}
}

func checkMemory() {
	conf := config.GetConfig()
	count := database.MemTree.Size()
	if count < conf.Threshold {
		return
	}
	log.Println("Compressing memory")
	tmpTree := database.MemTree
	database.MemTree = redblacktree.New[string, []byte]()
	database.TableTree.CreateNewTable(tmpTree.ToValue())
	database.Wal.Reset()
}
