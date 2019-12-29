package skipdisk

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"github.com/elliotcourant/buffers"
	"math/bits"
	"math/rand"
	"os"
	"path"
	"path/filepath"
	"sync"
	"sync/atomic"
)

const (
	MaxLevel     uint8   = 25
	nilReference nodeRef = 0
)

type (
	// nodeRef is a uint64 pointer to a single file, the first 32-Bits (4 bytes) represents the
	// fileId where the nodeRef is stored. The next 32-Bits (4 bytes) represents a start offset in
	// the file where the data can be read.
	nodeRef uint64

	// fileRef represents a uint32 pointer to a single file for the skiplist.
	fileRef uint32

	Node struct {
		ref   nodeRef // Is not written to the disk, is stored to re-write this to the disk.
		level uint8
		key   []byte
		next  [MaxLevel]nodeRef
		prev  nodeRef
	}

	segment struct {
		fileId fileRef
		space  uint32
		file   *os.File
	}

	DB struct {
		nextFileId   fileRef
		directory    string
		startLevels  [MaxLevel]nodeRef
		endLevels    [MaxLevel]nodeRef
		maxNewLevel  uint8
		maxLevel     uint8
		elementCount uint64

		segmentReadLock  sync.RWMutex
		segmentWriteLock sync.Mutex
		segments         map[fileRef]*segment

		root *os.File
	}
)

func (r *nodeRef) Get() (file fileRef, offset uint32) {
	current := uint64(*r)
	return fileRef(uint32(current >> 32)), uint32(current)
}

func (r *nodeRef) FileId() fileRef {
	current := uint64(*r)
	return fileRef(uint32(current >> 32))
}

func newReference(fileId fileRef, offset uint32) nodeRef {
	return nodeRef(uint64(fileId)<<32 | uint64(offset))
}

func (f fileRef) GetFilename() string {
	b := make([]byte, 4)
	binary.BigEndian.PutUint32(b, uint32(f))
	return fmt.Sprintf("%s.seg", hex.EncodeToString(b))
}

func NewDB(directory string) *DB {
	dir, err := filepath.Abs(directory)
	if err != nil {
		panic(err)
	}
	if err := newDirectory(dir); err != nil {
		panic(err)
	}
	if err := takeOwnership(dir); err != nil {
		panic(err)
	}

	db := &DB{
		nextFileId:   0,
		directory:    dir,
		startLevels:  [MaxLevel]nodeRef{},
		endLevels:    [MaxLevel]nodeRef{},
		maxNewLevel:  0,
		maxLevel:     0,
		elementCount: 0,
		segments:     map[fileRef]*segment{},
	}

	rootFileId := fileRef(0)

	db.readRoot(db.getPath(rootFileId))

	return db
}

func (db *DB) readRoot(path string) {
	takeOwnership(path)
	flags := os.O_CREATE | os.O_RDWR | os.O_SYNC
	mode := os.ModeAppend | os.ModePerm
	file, err := os.OpenFile(path, flags, mode)
	if err != nil {
		panic(err)
	}

	db.root = file

	stat, _ := file.Stat()
	if stat.Size() == 0 {
		db.nextFileId++
		return
	}

	offset := int64(0)

	for i := 0; i < int(MaxLevel); i++ {
		ref := make([]byte, 8)
		if _, err := file.ReadAt(ref, offset); err != nil {
			panic(err)
		}
		offset += 8

		db.startLevels[i] = nodeRef(binary.BigEndian.Uint64(ref))
	}

	for i := 0; i < int(MaxLevel); i++ {
		ref := make([]byte, 8)
		if _, err := file.ReadAt(ref, offset); err != nil {
			panic(err)
		}
		offset += 8

		db.endLevels[i] = nodeRef(binary.BigEndian.Uint64(ref))
	}

	src := make([]byte, 4+1+1+8)
	if _, err := file.ReadAt(src, offset); err != nil {
		panic(err)
	}
	buf := buffers.NewBytesReader(src)
	db.nextFileId = fileRef(buf.NextUint32())
	db.maxNewLevel = buf.NextUint8()
	db.maxLevel = buf.NextUint8()
	db.elementCount = buf.NextUint64()
}

func (db *DB) writeRoot() {
	buf := buffers.NewBytesBuffer()
	for _, ref := range db.startLevels {
		buf.AppendUint64(uint64(ref))
	}
	for _, ref := range db.endLevels {
		buf.AppendUint64(uint64(ref))
	}
	buf.AppendUint32(uint32(db.nextFileId))
	buf.AppendUint8(db.maxNewLevel)
	buf.AppendUint8(db.maxLevel)
	buf.AppendUint64(db.elementCount)
	db.root.WriteAt(buf.Bytes(), 0)
}

func (db *DB) IsEmpty() bool {
	return db.startLevels[0] == nilReference
}

func (db *DB) Close() {
	db.writeRoot()
	for _, segment := range db.segments {
		segment.Sync()
		segment.file.Close()
	}
}

func (db *DB) getPath(fileId fileRef) string {
	return path.Join(db.directory, fileId.GetFilename())
}

func (db *DB) generateLevel(maxLevel uint8) uint8 {
	level := MaxLevel - 1
	var x uint64 = rand.Uint64() & ((1 << uint(maxLevel-1)) - 1)
	zeros := uint8(bits.TrailingZeros64(x))
	if zeros <= maxLevel {
		level = zeros
	}

	return level
}

func (db *DB) findEntryIndex(key []byte, level uint8) uint8 {
	// TODO (elliotcourant) make sure the always true isnt a problem
	for i := db.maxLevel; i >= 0; i-- {
		node := db.getReference(db.startLevels[i])
		if node != nil && bytes.Compare(node.key, key) <= 0 || i <= level {
			return i
		}
	}

	return 0
}

func (db *DB) findExtended(key []byte, findGreaterOrEqual bool) (foundElem *Node, ok bool) {
	foundElem = nil
	ok = false

	if db.IsEmpty() {
		return
	}

	index := db.findEntryIndex(key, 0)
	var currentNode *Node

	currentNode = db.getReference(db.startLevels[index])
	nextNode := currentNode

	if findGreaterOrEqual && bytes.Compare(currentNode.key, key) > 0 {
		foundElem = currentNode
		ok = true
		return
	}

	for {
		if bytes.Compare(currentNode.key, key) == 0 {
			foundElem = currentNode
			ok = true
			return
		}

		nextNode = db.getReference(currentNode.next[index])

		// Which direction are we continuing next time?
		if nextNode != nil && bytes.Compare(nextNode.key, key) <= 0 {
			currentNode = nextNode
		} else {
			if index > 0 {

				if next := db.getReference(currentNode.next[0]); next != nil && bytes.Compare(nextNode.key, key) == 0 {
					foundElem = next
					ok = true
					return
				}

				index--
			} else {
				if findGreaterOrEqual {
					foundElem = nextNode
					ok = nextNode != nil
				}

				return
			}
		}
	}
}

func (db *DB) Find(key []byte) (node *Node, ok bool) {
	if db == nil || key == nil {
		return
	}

	return db.findExtended(key, false)
}

func (db *DB) FindGreaterOrEqual(key []byte) (node *Node, ok bool) {
	if db == nil || key == nil {
		return
	}

	return db.findExtended(key, true)
}

func (db *DB) Insert(key []byte) {
	if db == nil || key == nil {
		return
	}

	level := db.generateLevel(db.maxNewLevel)

	if level > db.maxLevel {
		level = db.maxLevel + 1
		db.maxLevel = level
	}

	elem := &Node{
		ref:   0,
		level: level,
		key:   key,
		next:  [25]nodeRef{},
		prev:  0,
	}

	db.currentSegment().Append(elem)

	db.elementCount++

	newFirst, newLast := true, true

	if !db.IsEmpty() {
		newFirst = bytes.Compare(elem.key, db.getReference(db.startLevels[0]).key) < 0
		newLast = bytes.Compare(elem.key, db.getReference(db.endLevels[0]).key) > 0
	}

	normallyInserted := false
	if !newFirst && !newLast {
		normallyInserted = true

		index := db.findEntryIndex(elem.key, level)

		var currentNode *Node
		var nextNode *Node
		// nextNode := db.getReference(db.startLevels[index])

		for {
			if currentNode == nil {
				nextNode = db.getReference(db.startLevels[index])
			} else {
				nextNode = db.getReference(currentNode.next[index])
			}

			// Connect node to next
			if index <= level && (nextNode == nil || bytes.Compare(nextNode.key, elem.key) > 0) {
				if nextNode != nil {
					db.updateNext(elem, index, nextNode.ref)
				}

				if currentNode != nil {
					db.updateNext(currentNode, index, elem.ref)
				}

				if index == 0 {
					if currentNode != nil {
						db.updatePrev(elem, currentNode.ref)
					} else {
						db.updatePrev(elem, nilReference)
					}

					if nextNode != nil {
						db.updatePrev(nextNode, elem.ref)
					}
				}
			}

			if nextNode != nil && bytes.Compare(nextNode.key, elem.key) < 0 {
				// Go Right
				currentNode = nextNode
			} else if nextNode != nil && bytes.Compare(nextNode.key, elem.key) == 0 {
				return
			} else {
				// Go Down
				index--
				if index < 0 || index == 255 {
					break
				}
			}
		}
	}

	for i := level; int8(i) >= 0; i-- {
		didSomething := false

		if newFirst || normallyInserted {
			if node := db.getReference(db.startLevels[i]); node == nil || bytes.Compare(node.key, elem.key) > 0 {
				if i == 0 && node != nil {
					db.updatePrev(node, elem.ref)
				}

				if node != nil {
					db.updateNext(elem, i, node.ref)
				} else {
					db.updateNext(elem, i, nilReference)
				}

				db.startLevels[i] = elem.ref
			}

			// link the endLevels to this element!
			if db.getReference(elem.next[i]) == nil {
				db.endLevels[i] = elem.ref
			}

			didSomething = true
		}

		if newLast {
			// Places the element after the very last element on this level!
			// This is very important, so we are not linking the very first element (newFirst AND newLast) to itself!
			if !newFirst {
				if node := db.getReference(db.endLevels[i]); node != nil {
					db.updateNext(node, i, elem.ref)
				}

				if i == 0 {
					db.updatePrev(elem, db.endLevels[i])
				}

				db.endLevels[i] = elem.ref
			}

			// Link the startLevels to this element!
			if node := db.getReference(db.startLevels[i]); node == nil || bytes.Compare(node.key, elem.key) > 0 {
				db.startLevels[i] = elem.ref
			}

			didSomething = true
		}

		if !didSomething {
			break
		}
	}
}

func (db *DB) updateNext(node *Node, index uint8, ref nodeRef) {
	node.next[index] = ref
	fileId, offset := node.ref.Get()
	db.getSegment(fileId).UpdateNext(offset, index, ref)
}

func (db *DB) updatePrev(node *Node, ref nodeRef) {
	node.prev = ref
	fileId, offset := node.ref.Get()
	db.getSegment(fileId).UpdatePrev(offset, ref)
}

func (db *DB) getReference(ref nodeRef) *Node {
	if ref == nilReference {
		return nil
	}

	fileId, offset := ref.Get()

	seg := db.getSegment(fileId)

	return seg.GetNode(offset)
}

func (db *DB) getSegment(fileId fileRef) *segment {
	db.segmentReadLock.RLock()
	seg, ok := db.segments[fileId]
	if !ok {
		db.segmentWriteLock.Lock()
		db.segmentReadLock.RUnlock()
		db.segmentReadLock.Lock()

		filePath := db.getPath(fileId)

		takeOwnership(filePath)

		flags := os.O_CREATE | os.O_RDWR | os.O_SYNC
		mode := os.ModeAppend | os.ModePerm
		file, err := os.OpenFile(filePath, flags, mode)
		if err != nil {
			panic(err)
		}

		space := uint32(0)

		stats, _ := file.Stat()

		if stats.Size() < 4 {
			// Empty file.
			space = 4
		} else {
			s := make([]byte, 4)
			if _, err := file.ReadAt(s, 0); err != nil {
				panic(err)
			}
			space = binary.BigEndian.Uint32(s)
		}

		seg = &segment{
			fileId: fileId,
			file:   file,
			space:  space,
		}

		db.segments[fileId] = seg
		db.segmentWriteLock.Unlock()
		db.segmentReadLock.Unlock()
	} else {
		db.segmentReadLock.RUnlock()
	}

	return seg
}

func (db *DB) currentSegment() *segment {
	return db.getSegment(db.nextFileId)
}

func (seg *segment) GetNode(offset uint32) *Node {
	sizeBinary := make([]byte, 2)
	start := int64(offset)
	if _, err := seg.file.ReadAt(sizeBinary, start); err != nil {
		panic(err)
	}
	size := binary.BigEndian.Uint16(sizeBinary)
	if size == 0 {
		return nil
	}
	nodeData := make([]byte, size-2)
	if _, err := seg.file.ReadAt(nodeData, int64(offset)+2); err != nil {
		panic(err)
	}

	return newNodeFromBytes(seg.fileId, offset, nodeData)
}

func (seg *segment) Append(node *Node) {
	size := node.Size()

	offset := atomic.AddUint32(&seg.space, uint32(size)) - uint32(size)
	ref := newReference(seg.fileId, offset)
	buf := node.Encode()
	if _, err := seg.file.WriteAt(buf, int64(offset)); err != nil {
		panic(err)
	}
	node.ref = ref
}

func (seg *segment) Sync() {
	s := make([]byte, 4)
	binary.BigEndian.PutUint32(s, seg.space)
	if _, err := seg.file.WriteAt(s, 0); err != nil {
		panic(err)
	}
	if err := seg.file.Sync(); err != nil {
		panic(err)
	}
}

func (seg *segment) UpdateNext(offset uint32, index uint8, ref nodeRef) {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, uint64(ref))

	// The next array is the first thing after the size prefix. So we can multiply the index by 8
	// which will give us the byte offset from the original starting offset + the size offset to get
	// the address of the next reference that we need to update.
	if _, err := seg.file.WriteAt(b, int64(offset)+2+(8*int64(index))); err != nil {
		panic(err)
	}
}

func (seg *segment) UpdatePrev(offset uint32, ref nodeRef) {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, uint64(ref))

	// We need to update the 8 bytes that follow the next array and the 2 byte size prefix. This
	// will update that by calculating the offset based on the MaxLevel constant.
	if _, err := seg.file.WriteAt(b, int64(offset)+2+(8*int64(MaxLevel))); err != nil {
		panic(err)
	}
}

func newNodeFromBytes(file fileRef, offset uint32, src []byte) *Node {
	node := Node{}
	node.ref = newReference(file, offset)
	buf := buffers.NewBytesReader(src)
	node.next = [MaxLevel]nodeRef{}
	for i := uint8(0); i < MaxLevel; i++ {
		node.next[i] = nodeRef(buf.NextUint64())
	}
	node.prev = nodeRef(buf.NextUint64())
	node.level = buf.NextByte()
	node.key = buf.NextBytes()
	return &node
}

func (n *Node) Size() uint16 {
	sizes := []uint16{
		2,                    // Size Prefix
		uint16(MaxLevel * 8), // 8 Bytes per level
		8,                    // Previous
		1,                    // Level
		4,                    // Key Size
		uint16(len(n.key)),   // Key
	}
	size := uint16(0)
	for _, s := range sizes {
		size += s
	}

	return size
}

func (n *Node) Encode() []byte {
	buf := buffers.NewBytesBuffer()
	buf.AppendUint16(0)
	for _, reference := range n.next {
		buf.AppendUint64(uint64(reference))
	}
	buf.AppendUint64(uint64(n.prev))
	buf.AppendUint8(n.level)
	buf.Append(n.key...)
	b := buf.Bytes()
	binary.BigEndian.PutUint16(b[0:2], uint16(len(b)))
	return b
}
