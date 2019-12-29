package skipdisk

type Iterator struct {
	db          *DB
	currentNode *Node
}

func (db *DB) NewIterator() *Iterator {
	return &Iterator{
		db:          db,
		currentNode: nil,
	}
}

func (i *Iterator) Rewind() {
	i.currentNode = i.db.getReference(i.db.startLevels[0])
}

func (i *Iterator) Seek(key []byte) {
	node, _ := i.db.FindGreaterOrEqual(key)
	i.currentNode = node
}

func (i *Iterator) Next() {
	node := i.db.getReference(i.currentNode.next[0])
	if node != nil {
		i.currentNode = node
	}
}

func (i *Iterator) Item() *Node {
	return i.currentNode
}
