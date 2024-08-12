package config

import (
	"sync"
)

type Config struct {
	// 目录
	DataDir string
	// 每层数据量，单位 MB
	LevelSize int
	//每层 sstable 表数量阈值
	PartSize int
	// 内存表最大 kv 数量，超过将被存储在 sstable
	Threshold int
	//压缩文件间隔
	CompressionInterval int
}

var once *sync.Once = &sync.Once{}

var config Config

func Init(tmp_config Config) {
	once.Do(func() { config = tmp_config })
}

func GetConfig() Config {
	return config
}
