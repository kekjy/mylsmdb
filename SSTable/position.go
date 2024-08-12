package sstable

type Position struct {
	Start int64
	Len   int64
	Del   bool
}
