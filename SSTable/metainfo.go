package sstable

type MetaInfo struct {
	version     int64
	data_start  int64
	data_len    int64
	index_start int64
	index_len   int64
}

const METAINFOINT = uint8(64)
