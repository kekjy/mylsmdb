package benchmark

import (
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	lsm "mylsmdb"
	"mylsmdb/config"
	"mylsmdb/utils"

	"github.com/stretchr/testify/assert"
)

func openDB() {
	lsm.Start(config.Config{
		DataDir:             `E:\00database`,
		LevelSize:           10,
		PartSize:            4,
		Threshold:           200000,
		CompressionInterval: 10, // 压缩时间间隔
	})
}

var (
	lock    = sync.Mutex{}
	randStr = rand.New(rand.NewSource(time.Now().Unix()))
	letters = []byte("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")
)

// GetTestKey get formatted key, for test only
func GetTestKey(i int) []byte {
	return []byte(fmt.Sprintf("mylsmdb-test-key-%09d", i))
}

// RandomValue generate random value, for test only
func RandomValue(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		lock.Lock()
		b[i] = letters[randStr.Intn(len(letters))]
		lock.Unlock()
	}
	return []byte("mylsmdb-test-value-" + string(b))
}

func BenchmarkPutGet(b *testing.B) {
	openDB()

	b.Run("put", benchmarkPut)
	b.Run("get", bencharkGet)
}

func BenchmarkBatchPutGet(b *testing.B) {

	b.Run("batchPut", benchmarkBatchPut)
	b.Run("batchGet", benchmarkBatchGet)
}

func benchmarkPut(b *testing.B) {
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		found := lsm.Set(utils.ToString(GetTestKey(i)), RandomValue(1024))
		if !found {
			assert.Nil(b, found)
		}
	}
}

func benchmarkBatchPut(b *testing.B) {
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		found := lsm.Set(utils.ToString(GetTestKey(i)), RandomValue(1024))
		if !found {
			assert.Nil(b, found)
		}
	}
}

func benchmarkBatchGet(b *testing.B) {
	for i := 0; i < 10000; i++ {
		found := lsm.Set(utils.ToString(GetTestKey(i)), RandomValue(1024))
		if !found {
			assert.Nil(b, found)
		}
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, _ = lsm.Get[[]byte](utils.ToString(GetTestKey(rand.Int())))
	}
}

func bencharkGet(b *testing.B) {
	for i := 0; i < 10000; i++ {
		found := lsm.Set(utils.ToString(GetTestKey(i)), RandomValue(1024))
		if !found {
			assert.Nil(b, found)
		}
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, _ = lsm.Get[[]byte](utils.ToString(GetTestKey(rand.Int())))
	}
}
