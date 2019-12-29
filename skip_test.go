package skipdisk

import (
	"encoding/binary"
	"fmt"
	"testing"
)

func TestSkip(t *testing.T) {
	db := NewDB("")
	defer db.Close()

	for i := 0; i < 1000; i++ {
		key := make([]byte, 4)
		binary.BigEndian.PutUint32(key, uint32(i))
		db.Insert(key)
	}

	fmt.Println("test")
}
