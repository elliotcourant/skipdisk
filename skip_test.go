package skipdisk

import (
	"encoding/binary"
	"fmt"
	"testing"
	"time"
)

func TestSkip(t *testing.T) {
	db := NewDB("")
	defer db.Close()

	for i := 0; i < 1000; i++ {
		k := make([]byte, 8)
		binary.BigEndian.PutUint64(k, uint64(i))
		db.Insert(k)
	}

	itr := db.NewIterator()

	start := time.Now()
	itr.Rewind()
	for i := 0; i < 1000; i++ {
		fmt.Printf("Address: %d Key: %v\n", itr.Item().ref, itr.Item().key)
		itr.Next()
	}
	fmt.Println("iteration took:", time.Since(start))

	fmt.Println("test")
}
