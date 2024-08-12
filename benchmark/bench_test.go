package benchmark

import (
	"math/rand"
	"testing"

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
		Threshold:           10000,
		CompressionInterval: 10, // 压缩时间间隔
	})
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
		found := lsm.Set(utils.ToString(utils.GetTestKey(i)), utils.RandomValue(1024))
		if !found {
			assert.Nil(b, found)
		}
	}
}

func benchmarkBatchPut(b *testing.B) {
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		found := lsm.Set(utils.ToString(utils.GetTestKey(i)), utils.RandomValue(1024))
		if !found {
			assert.Nil(b, found)
		}
	}
}

func benchmarkBatchGet(b *testing.B) {
	for i := 0; i < 10000; i++ {
		found := lsm.Set(utils.ToString(utils.GetTestKey(i)), utils.RandomValue(1024))
		if !found {
			assert.Nil(b, found)
		}
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, _ = lsm.Get[[]byte](utils.ToString(utils.GetTestKey(rand.Int())))
	}
}

func bencharkGet(b *testing.B) {
	for i := 0; i < 10000; i++ {
		found := lsm.Set(utils.ToString(utils.GetTestKey(i)), utils.RandomValue(1024))
		if !found {
			assert.Nil(b, found)
		}
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, _ = lsm.Get[[]byte](utils.ToString(utils.GetTestKey(rand.Int())))
	}
}
