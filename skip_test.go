package skipdisk

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"testing"
	"time"
)

func TestSkip(t *testing.T) {
	db := NewDB("data")
	defer db.Close()

	keys := make([][]byte, 100)
	for i := range keys {
		k := make([]byte, 8)
		binary.BigEndian.PutUint64(k, uint64(i))
		keys[i] = k
	}

	start := time.Now()
	for _, key := range keys {
		db.Insert(key)
	}
	end := time.Since(start)
	fmt.Println("insert took:", end, "at", time.Duration(int64(end)/int64(len(keys))), "per insert, and",
		float64(len(keys))/end.Minutes(), "inserts per minute")

	itr := db.NewIterator()

	start = time.Now()
	itr.Rewind()
	for i := 0; i < len(keys); i++ {
		if bytes.Compare(keys[i], itr.Item().key) != 0 {
			panic("invalid order")
		}
		itr.Next()
	}
	end = time.Since(start)
	fmt.Println("iteration took:", end)

	fmt.Println("test")
}
