package skipdisk

import (
	"fmt"
	"testing"
	"time"
)

func TestSkip(t *testing.T) {
	db := NewDB("")
	defer db.Close()

	db.Insert([]byte("a"))
	itr := db.NewIterator()

	start := time.Now()
	itr.Rewind()
	for i := 0; i < 10; i++ {
		fmt.Printf("Address: %d Key: %s\n", itr.Item().ref, string(itr.Item().key))
		itr.Next()
	}
	fmt.Println("iteration took:", time.Since(start))

	fmt.Println("test")
}
