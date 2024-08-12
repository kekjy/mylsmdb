package kv

import (
	"encoding/json"
)

type SearchResult byte

const (
	Delete SearchResult = iota
	Success
	None
)

type Value struct {
	Key   string
	Value []byte
	Del   bool
}

func (v *Value) Copy() *Value {
	return &Value{
		Key:   v.Key,
		Value: v.Value,
		Del:   v.Del,
	}
}

func Encode[T any](val T) ([]byte, error) {
	return json.Marshal(val)
}

func Decode[T any](v []byte) (T, error) {
	var val T
	err := json.Unmarshal(v, &val)
	return val, err
}
