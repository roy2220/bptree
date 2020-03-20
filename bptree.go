// Package bptree implements an in-memory B+ tree.
package bptree

import (
	"errors"
	"unsafe"
)

const (
	// KeyMin is the minimum key in a B+ tree at present.
	KeyMin = keyMinMax(-1)

	// KeyMax is the maximum key in a B+ tree at present.
	KeyMax = keyMinMax(1)
)

// BPTree represents a B+ tree.
type BPTree struct {
	maxDegree   int
	keyComparer KeyComparer
	leafList    leafList
	root        unsafe.Pointer
	height      int
}

// Init initializes the B+ tree with the given maximum degree
// and key comparer and returns it.
func (bpt *BPTree) Init(maxDegree int, keyComparer KeyComparer) *BPTree {
	if maxDegree < 4 {
		panic(errors.New("bptree: invalid maximum degree"))
	}

	bpt.maxDegree = maxDegree
	bpt.keyComparer = keyComparer
	root := leaf{}
	bpt.leafList.Init(&root)
	bpt.root = unsafe.Pointer(&root)
	bpt.height = 1
	return bpt
}

// AddRecord adds the given record to the B+ tree.
// If no record with an identical key exists in the B+ tree,
// it adds the record then returns true, otherwise it returns
// false and the present value of the record.
func (bpt *BPTree) AddRecord(key, value interface{}) (interface{}, bool) {
	recordPath, ok := bpt.findRecord(key)

	if ok {
		leaf, recordIndex := recordPath.LocateRecord()
		return leaf.Records()[recordIndex].Value, false
	}

	bpt.insertRecord(record{key, value}, recordPath)
	return nil, true
}

// UpdateRecord replaces the value of a record with the given
// key in the B+ tree to the given one.
// If a record with an identical key exists in the B+ tree,
// it updates the record then returns true and the replaced
// value of the record, otherwise it returns flase.
func (bpt *BPTree) UpdateRecord(key, value interface{}) (interface{}, bool) {
	if recordPath, ok := bpt.findRecord(key); ok {
		leaf, recordIndex := recordPath.LocateRecord()
		leaf.Records()[recordIndex].Value, value = value, leaf.Records()[recordIndex].Value
		return value, true
	}

	return nil, false
}

// AddOrUpdateRecord adds the given record to the B+ tree or
// replaces the value of a record with the given key to the
// given one.
// If no record with an identical key exists in the B+ tree,
// it adds the record then returns true, otherwise if updates
// the record then returns false and the replaced value of the
// record.
func (bpt *BPTree) AddOrUpdateRecord(key, value interface{}) (interface{}, bool) {
	recordPath, ok := bpt.findRecord(key)

	if ok {
		leaf, recordIndex := recordPath.LocateRecord()
		leaf.Records()[recordIndex].Value, value = value, leaf.Records()[recordIndex].Value
		return value, false
	}

	bpt.insertRecord(record{key, value}, recordPath)
	return nil, true
}

// DeleteRecord deletes a record with the given key in the
// B+ tree.
// If a record with an identical key exists in the B+ tree,
// it deletes the record then returns true and the removed
// value of the record, otherwise it returns flase.
func (bpt *BPTree) DeleteRecord(key interface{}) (interface{}, bool) {
	if recordPath, ok := bpt.findRecord(key); ok {
		leaf, recordIndex := recordPath.LocateRecord()
		value := leaf.Records()[recordIndex].Value
		bpt.removeRecord(recordPath)
		return value, true
	}

	return nil, false
}

// HasRecord checks whether a record with the given key
// in the B+ tree.
// If a record with an identical key exists in the B+ tree,
// it returns true and the present value of the record,
// otherwise it returns flase.
func (bpt *BPTree) HasRecord(key interface{}) (interface{}, bool) {
	if recordPath, ok := bpt.findRecord(key); ok {
		leaf, recordIndex := recordPath.LocateRecord()
		return leaf.Records()[recordIndex].Value, true
	}

	return nil, false
}

// SearchForward searchs the the B+ tree for records with
// keys in the given interval [maxKey, minKey].
// It returns an iterator to iterate over the records found
// in a ascending order.
func (bpt *BPTree) SearchForward(maxKey, minKey interface{}) Iterator {
	minLeaf, minRecordIndex, maxLeaf, maxRecordIndex, ok := bpt.findAndLocateRecords(maxKey, minKey)
	return new(forwardIterator).Init(minLeaf, minRecordIndex, maxLeaf, maxRecordIndex, !ok)
}

// SearchBackward searchs the the B+ tree for records with
// keys in the given interval [maxKey, minKey].
// It returns an iterator to iterate over the records found
// in a descending order.
func (bpt *BPTree) SearchBackward(maxKey, minKey interface{}) Iterator {
	minLeaf, minRecordIndex, maxLeaf, maxRecordIndex, ok := bpt.findAndLocateRecords(maxKey, minKey)
	return new(backwardIterator).Init(maxLeaf, maxRecordIndex, minLeaf, minRecordIndex, !ok)
}

// IsEmpty indicates whether the B+ tree is empty.
func (bpt *BPTree) IsEmpty() bool {
	return bpt.height == 1 && len((*leaf)(bpt.root).Records()) == 0
}

// MaxDegree returns the maximum degree of the B+ tree.
func (bpt *BPTree) MaxDegree() int {
	return bpt.maxDegree
}

// Height returns the height of the B+ tree.
func (bpt *BPTree) Height() int {
	return bpt.height
}

func (bpt *BPTree) findRecord(key interface{}) (recordPath, bool) {
	recordPath := recordPath(make([]recordPathComponent, 0, bpt.height+1))
	node := bpt.root

	for {
		if nodeDepth := len(recordPath) + 1; nodeDepth == bpt.height {
			leaf := (*leaf)(node)
			i, ok := leaf.LocateRecord(key, bpt.keyComparer)
			recordPath.Append(node, i)
			return recordPath, ok
		}

		nonLeaf := (*nonLeaf)(node)
		i, ok := nonLeaf.LocateChild(key, bpt.keyComparer)

		if !ok {
			i--
		}

		recordPath.Append(node, i)
		node = nonLeaf.Children()[i].Value
	}
}

func (bpt *BPTree) insertRecord(record record, recordPath recordPath) {
	bpt.ensureNotFullLeaf(&recordPath)
	leaf, recordIndex := recordPath.LocateRecord()
	leaf.InsertRecord(record, recordIndex)
	syncKey(recordPath)
}

func (bpt *BPTree) removeRecord(recordPath recordPath) {
	if len(recordPath) >= 2 {
		bpt.ensureNotSparseLeaf(&recordPath)
	}

	leaf, recordIndex := recordPath.LocateRecord()
	leaf.RemoveRecord(recordIndex)
	syncKey(recordPath)
}

func (bpt *BPTree) ensureNotFullLeaf(recordPath *recordPath) {
	i := len(*recordPath) - 1
	leaf1 := (*recordPath)[i].Leaf()

	if !leaf1.IsFull(bpt.maxDegree) {
		return
	}

	recordIndex := (*recordPath)[i].RecordIndex()
	numberOfRecords := bpt.maxDegree / 2

	if i == 0 {
		bpt.increaseHeight()
		recordPath.Prepend(bpt.root, 0)
		i = 1
	}

	leafParent := (*recordPath)[i-1].NonLeaf()
	leafIndex := (*recordPath)[i-1].NodeChildIndex()

	if leafIndex < len(leafParent.Children())-1 {
		leafRightSibling := (*leaf)(leafParent.Children()[leafIndex+1].Value)

		if !leafRightSibling.IsFull(bpt.maxDegree) {
			if recordIndex == len(leaf1.Records()) {
				(*recordPath)[i].SetLeaf(leafRightSibling)
				(*recordPath)[i].SetRecordIndex(0)
				(*recordPath)[i-1].SetNodeChildIndex(leafIndex + 1)
			} else {
				leaf1.ShiftToRight(leafParent, leafIndex, leafRightSibling)
			}

			return
		}
	}

	if leafIndex >= 1 {
		leafLeftSibling := (*leaf)(leafParent.Children()[leafIndex-1].Value)

		if !leafLeftSibling.IsFull(bpt.maxDegree) {
			// if recordIndex == 0 {
			//	(*recordPath)[i].SetLeaf(leafLeftSibling)
			//	(*recordPath)[i].SetRecordIndex(len(leafLeftSibling.Records()))
			//	(*recordPath)[i-1].SetNodeChildIndex(leafIndex - 1)
			// } else {
			leaf1.ShiftToLeft(leafParent, leafIndex, leafLeftSibling)
			(*recordPath)[i].SetRecordIndex(recordIndex - 1)
			// }

			return
		}
	}

	i = bpt.ensureNotFullNonLeaf(recordPath, i-1) + 1
	leafParent = (*recordPath)[i-1].NonLeaf()
	leafIndex = (*recordPath)[i-1].NodeChildIndex()
	leafNewSibling := leaf1.Split(numberOfRecords, leafParent, leafIndex)
	bpt.leafList.InsertLeafAfter(leafNewSibling, leaf1)

	if recordIndex >= numberOfRecords {
		(*recordPath)[i].SetLeaf(leafNewSibling)
		(*recordPath)[i].SetRecordIndex(recordIndex - numberOfRecords)
		(*recordPath)[i-1].SetNodeChildIndex(leafIndex + 1)
	}
}

func (bpt *BPTree) ensureNotFullNonLeaf(recordPath *recordPath, i int) int {
	nonLeaf1 := (*recordPath)[i].NonLeaf()

	if !nonLeaf1.IsFull(bpt.maxDegree) {
		return i
	}

	nonLeafChildIndex := (*recordPath)[i].NodeChildIndex()
	numberOfNonLeafChildren := 1 + (bpt.maxDegree-1)/2

	if i == 0 {
		bpt.increaseHeight()
		recordPath.Prepend(bpt.root, 0)
		i = 1
	}

	nonLeafParent := (*recordPath)[i-1].NonLeaf()
	nonLeafIndex := (*recordPath)[i-1].NodeChildIndex()

	if nonLeafIndex < len(nonLeafParent.Children())-1 {
		nonLeafRightSibling := (*nonLeaf)(nonLeafParent.Children()[nonLeafIndex+1].Value)

		if !nonLeafRightSibling.IsFull(bpt.maxDegree) {
			nonLeaf1.ShiftToRight(nonLeafParent, nonLeafIndex, nonLeafRightSibling)

			if nonLeafChildIndex == len(nonLeaf1.Children()) {
				(*recordPath)[i].SetNonLeaf(nonLeafRightSibling)
				(*recordPath)[i].SetNodeChildIndex(0)
				(*recordPath)[i-1].SetNodeChildIndex(nonLeafIndex + 1)
			}

			return i
		}
	}

	if nonLeafIndex >= 1 {
		nonLeafLeftSibling := (*nonLeaf)(nonLeafParent.Children()[nonLeafIndex-1].Value)

		if !nonLeafLeftSibling.IsFull(bpt.maxDegree) {
			nonLeaf1.ShiftToLeft(nonLeafParent, nonLeafIndex, nonLeafLeftSibling)

			if nonLeafChildIndex == 0 {
				(*recordPath)[i].SetNonLeaf(nonLeafLeftSibling)
				(*recordPath)[i].SetNodeChildIndex(len(nonLeafLeftSibling.Children()) - 1)
				(*recordPath)[i-1].SetNodeChildIndex(nonLeafIndex - 1)
			} else {
				(*recordPath)[i].SetNodeChildIndex(nonLeafChildIndex - 1)
			}

			return i
		}
	}

	i = bpt.ensureNotFullNonLeaf(recordPath, i-1) + 1
	nonLeafParent = (*recordPath)[i-1].NonLeaf()
	nonLeafIndex = (*recordPath)[i-1].NodeChildIndex()
	nonLeafNewSibling := nonLeaf1.Split(numberOfNonLeafChildren, nonLeafParent, nonLeafIndex)

	if nonLeafChildIndex >= numberOfNonLeafChildren {
		(*recordPath)[i].SetNonLeaf(nonLeafNewSibling)
		(*recordPath)[i].SetNodeChildIndex(nonLeafChildIndex - numberOfNonLeafChildren)
		(*recordPath)[i-1].SetNodeChildIndex(nonLeafIndex + 1)
	}

	return i
}

func (bpt *BPTree) ensureNotSparseLeaf(recordPath *recordPath) {
	i := len(*recordPath) - 1
	leaf1 := (*recordPath)[i].Leaf()

	if !leaf1.IsSparse(bpt.maxDegree) {
		return
	}

	recordIndex := (*recordPath)[i].RecordIndex()
	leafParent := (*recordPath)[i-1].NonLeaf()
	leafIndex := (*recordPath)[i-1].NodeChildIndex()
	var leafRightSibling *leaf

	if leafIndex < len(leafParent.Children())-1 {
		leafRightSibling = (*leaf)(leafParent.Children()[leafIndex+1].Value)

		if !leafRightSibling.IsSparse(bpt.maxDegree) {
			leaf1.UnshiftFromRight(leafParent, leafIndex, leafRightSibling)
			return
		}
	} else {
		leafRightSibling = nil
	}

	var leafLeftSibling *leaf

	if leafIndex >= 1 {
		leafLeftSibling = (*leaf)(leafParent.Children()[leafIndex-1].Value)

		if !leafLeftSibling.IsSparse(bpt.maxDegree) {
			leaf1.UnshiftFromLeft(leafParent, leafIndex, leafLeftSibling)
			(*recordPath)[i].SetRecordIndex(recordIndex + 1)
			return
		}
	} else {
		leafLeftSibling = nil
	}

	if i >= 2 {
		i = bpt.ensureNotSparseNonLeaf(recordPath, i-1) + 1
		leafParent = (*recordPath)[i-1].NonLeaf()
		leafIndex = (*recordPath)[i-1].NodeChildIndex()
	}

	if leafRightSibling != nil {
		leaf1.MergeFromRight(leafParent, leafIndex, leafRightSibling)
		bpt.leafList.RemoveLeaf(leafRightSibling)
	} else {
		numberOfRecords := len(leaf1.Records())
		leaf1.MergeToLeft(leafParent, leafIndex, leafLeftSibling)
		bpt.leafList.RemoveLeaf(leaf1)
		(*recordPath)[i].SetLeaf(leafLeftSibling)
		(*recordPath)[i].SetRecordIndex(len(leafLeftSibling.Records()) - (numberOfRecords - recordIndex))
		(*recordPath)[i-1].SetNodeChildIndex(leafIndex - 1)
	}

	if i == 1 && len(leafParent.Children()) == 1 {
		bpt.decreaseHeight()
		recordPath.Unprepend()
	}
}

func (bpt *BPTree) ensureNotSparseNonLeaf(recordPath *recordPath, i int) int {
	nonLeaf1 := (*recordPath)[i].NonLeaf()

	if !nonLeaf1.IsSparse(bpt.maxDegree) {
		return i
	}

	nonLeafChildIndex := (*recordPath)[i].NodeChildIndex()
	nonLeafParent := (*recordPath)[i-1].NonLeaf()
	nonLeafIndex := (*recordPath)[i-1].NodeChildIndex()
	var nonLeafRightSibling *nonLeaf
	var nonLeafLeftSibling *nonLeaf

	if nonLeafIndex < len(nonLeafParent.Children())-1 {
		nonLeafRightSibling = (*nonLeaf)(nonLeafParent.Children()[nonLeafIndex+1].Value)

		if !nonLeafRightSibling.IsSparse(bpt.maxDegree) {
			nonLeaf1.UnshiftFromRight(nonLeafParent, nonLeafIndex, nonLeafRightSibling)
			return i
		}
	} else {
		nonLeafRightSibling = nil
	}

	if nonLeafIndex >= 1 {
		nonLeafLeftSibling = (*nonLeaf)(nonLeafParent.Children()[nonLeafIndex-1].Value)

		if !nonLeafLeftSibling.IsSparse(bpt.maxDegree) {
			nonLeaf1.UnshiftFromLeft(nonLeafParent, nonLeafIndex, nonLeafLeftSibling)
			(*recordPath)[i].SetNodeChildIndex(nonLeafChildIndex + 1)
			return i
		}
	} else {
		nonLeafLeftSibling = nil
	}

	if i >= 2 {
		i = bpt.ensureNotSparseNonLeaf(recordPath, i-1) + 1
		nonLeafParent = (*recordPath)[i-1].NonLeaf()
		nonLeafIndex = (*recordPath)[i-1].NodeChildIndex()
	}

	if nonLeafRightSibling != nil {
		nonLeaf1.MergeFromRight(nonLeafParent, nonLeafIndex, nonLeafRightSibling)
	} else {
		numberOfNonLeafChildren := len(nonLeaf1.Children())
		nonLeaf1.MergeToLeft(nonLeafParent, nonLeafIndex, nonLeafLeftSibling)
		(*recordPath)[i].SetNonLeaf(nonLeafLeftSibling)
		(*recordPath)[i].SetNodeChildIndex(len(nonLeafLeftSibling.Children()) - (numberOfNonLeafChildren - nonLeafChildIndex))
		(*recordPath)[i-1].SetNodeChildIndex(nonLeafIndex - 1)
	}

	if i == 1 && len(nonLeafParent.Children()) == 1 {
		bpt.decreaseHeight()
		recordPath.Unprepend()
		return 0
	}

	return i
}

func (bpt *BPTree) increaseHeight() {
	root := nonLeaf{}
	root.InsertChild(nodeChild{nil, bpt.root}, 0)
	bpt.root = unsafe.Pointer(&root)
	bpt.height++
}

func (bpt *BPTree) decreaseHeight() {
	bpt.root = (*nonLeaf)(bpt.root).Children()[0].Value
	bpt.height--
}

func (bpt *BPTree) findAndLocateRecords(minKey interface{}, maxKey interface{}) (*leaf, int, *leaf, int, bool) {
	x, ok1 := minKey.(keyMinMax)
	y, ok2 := maxKey.(keyMinMax)
	var d int64

	if ok1 || ok2 {
		if ok1 && ok2 && x == y {
			d = 0
		} else {
			d = -1
		}
	} else {
		d = bpt.keyComparer(minKey, maxKey)

		if d > 0 {
			return nil, 0, nil, 0, false
		}
	}

	minRecordPath, ok3 := bpt.findRecord(minKey)
	minLeaf, minRecordIndex := recordPath.LocateRecord(minRecordPath)

	if !ok3 {
		if minRecordIndex == len(minLeaf.Records()) {
			if minLeaf == bpt.leafList.Tail() {
				return nil, 0, nil, 0, false
			}

			minLeaf = minLeaf.Next
			minRecordIndex = 0
		}
	}

	if d == 0 {
		return minLeaf, minRecordIndex, minLeaf, minRecordIndex, true
	}

	if ok1 || !ok3 {
		minKey = minLeaf.Records()[minRecordIndex].Key

		if !ok2 {
			d = bpt.keyComparer(minKey, maxKey)

			if d == 0 {
				return minLeaf, minRecordIndex, minLeaf, minRecordIndex, true
			}

			if d > 0 {
				return nil, 0, nil, 0, false
			}
		}
	}

	maxRecordPath, ok4 := bpt.findRecord(maxKey)
	maxLeaf, maxRecordIndex := recordPath.LocateRecord(maxRecordPath)

	if !ok4 {
		// if maxRecordIndex == 0 {
		//	if maxLeaf == bpt.leafList.Head() {
		//		return nil, 0, nil, 0, false
		//	}

		//	maxLeaf = maxLeaf.Prev
		//	maxRecordIndex = len(maxLeaf.Records()) - 1
		// } else {
		maxRecordIndex--
		// }
	}

	if ok2 || !ok4 {
		maxKey = maxLeaf.Records()[maxRecordIndex].Key
		d = bpt.keyComparer(minKey, maxKey)

		if d == 0 {
			return minLeaf, minRecordIndex, minLeaf, minRecordIndex, true
		}

		if d > 0 {
			return nil, 0, nil, 0, false
		}
	}

	return minLeaf, minRecordIndex, maxLeaf, maxRecordIndex, true
}

// KeyComparer compares two keys and returns an integer
// with a value < 0 means the key 1 is less than the key 2,
// with a value == 0 means the key 1 is equal to the key 2,
// with a value > 0 means the key 1 is greater to the key 2.
type KeyComparer func(key1, key2 interface{}) (delta int64)

type keyMinMax int

type leaf struct {
	records

	Prev *leaf
	Next *leaf
}

func (l *leaf) Split(numberOfRecords int, parent *nonLeaf, index int) *leaf {
	newSibling := leaf{
		records: make([]record, len(l.records)-numberOfRecords),
	}

	copy(newSibling.records, l.records[numberOfRecords:])
	l.Truncate(numberOfRecords)
	parent.InsertChild(nodeChild{newSibling.records[0].Key, unsafe.Pointer(&newSibling)}, index+1)
	return &newSibling
}

func (l *leaf) MergeToLeft(parent *nonLeaf, index int, leftSibling *leaf) {
	leftSibling.MergeFromRight(parent, index-1, l)
}

func (l *leaf) MergeFromRight(parent *nonLeaf, index int, rightSibling *leaf) {
	parent.RemoveChild(index + 1)
	l.records = append(l.records, rightSibling.records...)
	rightSibling.Truncate(0)
}

func (l *leaf) UnshiftFromLeft(parent *nonLeaf, index int, leftSibling *leaf) {
	leftSibling.ShiftToRight(parent, index-1, l)
}

func (l *leaf) UnshiftFromRight(parent *nonLeaf, index int, rightSibling *leaf) {
	rightSibling.ShiftToLeft(parent, index+1, l)
}

func (l *leaf) ShiftToLeft(parent *nonLeaf, index int, leftSibling *leaf) {
	record := l.RemoveRecord(0)
	parent.Children()[index].Key = l.records[0].Key
	leftSibling.InsertRecord(record, len(leftSibling.records))
}

func (l *leaf) ShiftToRight(parent *nonLeaf, index int, rightSibling *leaf) {
	record := l.RemoveRecord(len(l.records) - 1)
	rightSibling.InsertRecord(record, 0)
	parent.Children()[index+1].Key = record.Key
}

func (l *leaf) Records() []record {
	return l.records
}

type records []record

func (rs records) LocateRecord(key interface{}, keyComparer KeyComparer) (int, bool) {
	n := len(rs)

	if n == 0 {
		return 0, false
	}

	if x, ok := key.(keyMinMax); ok {
		var i int

		if x == KeyMin {
			i = 0
		} else {
			i = n - 1
		}

		return i, true
	}

	i, j := 0, n-1

	for i < j {
		k := (i + j) / 2
		// i <= k < j

		if keyComparer(rs[k].Key, key) < 0 {
			i = k + 1
			// i <= j
		} else {
			j = k
			// j >= i
		}
	}
	// i == j

	d := keyComparer(rs[i].Key, key)

	if d == 0 {
		return i, true
	}

	if d < 0 && i == n-1 {
		i = n
	}

	return i, false
}

func (rs *records) InsertRecord(record1 record, recordIndex int) {
	*rs = append(*rs, record{})
	copy((*rs)[recordIndex+1:], (*rs)[recordIndex:])
	(*rs)[recordIndex] = record1
}

func (rs *records) RemoveRecord(recordIndex int) record {
	record1 := (*rs)[recordIndex]
	copy((*rs)[recordIndex:], (*rs)[recordIndex+1:])
	(*rs)[len(*rs)-1] = record{}
	*rs = (*rs)[:len(*rs)-1]
	return record1
}

func (rs *records) Truncate(length int) {
	for i := len(*rs) - 1; i >= length; i-- {
		(*rs)[i] = record{}
	}

	*rs = (*rs)[:length]
}

func (rs records) IsSparse(maxDegree int) bool {
	return len(rs)*2 <= maxDegree
}

func (rs records) IsFull(maxDegree int) bool {
	return len(rs) == maxDegree
}

type record struct {
	Key   interface{}
	Value interface{}
}

type nonLeaf struct {
	nodeChildren
}

func (nl *nonLeaf) Split(numberOfChildren int, parent *nonLeaf, index int) *nonLeaf {
	newSibling := nonLeaf{
		nodeChildren: make([]nodeChild, len(nl.nodeChildren)-numberOfChildren),
	}

	copy(newSibling.nodeChildren, nl.nodeChildren[numberOfChildren:])
	key := newSibling.nodeChildren[0].Key
	newSibling.nodeChildren[0].Key = nil
	nl.Truncate(numberOfChildren)
	parent.InsertChild(nodeChild{key, unsafe.Pointer(&newSibling)}, index+1)
	return &newSibling
}

func (nl *nonLeaf) MergeToLeft(parent *nonLeaf, index int, leftSibling *nonLeaf) {
	leftSibling.MergeFromRight(parent, index-1, nl)
}

func (nl *nonLeaf) MergeFromRight(parent *nonLeaf, index int, rightSibling *nonLeaf) {
	rightSibling.nodeChildren[0].Key = parent.nodeChildren[index+1].Key
	parent.RemoveChild(index + 1)
	nl.nodeChildren = append(nl.nodeChildren, rightSibling.nodeChildren...)
	rightSibling.Truncate(0)
}

func (nl *nonLeaf) UnshiftFromLeft(parent *nonLeaf, index int, leftSibling *nonLeaf) {
	leftSibling.ShiftToRight(parent, index-1, nl)
}

func (nl *nonLeaf) UnshiftFromRight(parent *nonLeaf, index int, rightSibling *nonLeaf) {
	rightSibling.ShiftToLeft(parent, index+1, nl)
}

func (nl *nonLeaf) ShiftToLeft(parent *nonLeaf, index int, leftSibling *nonLeaf) {
	child := nl.RemoveChild(0)
	child.Key = parent.nodeChildren[index].Key
	parent.nodeChildren[index].Key = nl.nodeChildren[0].Key
	nl.nodeChildren[0].Key = nil
	leftSibling.InsertChild(child, len(leftSibling.nodeChildren))
}

func (nl *nonLeaf) ShiftToRight(parent *nonLeaf, index int, rightSibling *nonLeaf) {
	child := nl.RemoveChild(len(nl.nodeChildren) - 1)
	rightSibling.nodeChildren[0].Key = parent.nodeChildren[index+1].Key
	parent.nodeChildren[index+1].Key = child.Key
	child.Key = nil
	rightSibling.InsertChild(child, 0)
}

func (nl *nonLeaf) Children() []nodeChild {
	return nl.nodeChildren
}

type nodeChildren []nodeChild

func (nc nodeChildren) LocateChild(key interface{}, keyComparer KeyComparer) (int, bool) {
	n := len(nc)

	if x, ok := key.(keyMinMax); ok {
		var i int

		if x == KeyMin {
			i = 0
		} else {
			i = n - 1
		}

		return i, true
	}

	i, j := 1 /* skip the first child whose key is dummy */, n-1

	for i < j {
		k := (i + j) / 2
		// i <= k < j

		if keyComparer(nc[k].Key, key) < 0 {
			i = k + 1
			// i <= j
		} else {
			j = k
			// j >= i
		}
	}
	// i == j

	d := keyComparer(nc[i].Key, key)

	if d == 0 {
		return i, true
	}

	if d < 0 && i == n-1 {
		i = n
	}

	return i, false
}

func (nc *nodeChildren) InsertChild(nodeChild1 nodeChild, nodeChildIndex int) {
	*nc = append(*nc, nodeChild{})
	copy((*nc)[nodeChildIndex+1:], (*nc)[nodeChildIndex:])
	(*nc)[nodeChildIndex] = nodeChild1
}

func (nc *nodeChildren) RemoveChild(nodeChildIndex int) nodeChild {
	nodeChild1 := (*nc)[nodeChildIndex]
	copy((*nc)[nodeChildIndex:], (*nc)[nodeChildIndex+1:])
	(*nc)[len(*nc)-1] = nodeChild{}
	*nc = (*nc)[:len(*nc)-1]
	return nodeChild1
}

func (nc *nodeChildren) Truncate(length int) {
	for i := len(*nc) - 1; i >= length; i-- {
		(*nc)[i] = nodeChild{}
	}

	*nc = (*nc)[:length]
}

func (nc nodeChildren) IsSparse(maxDegree int) bool {
	return len(nc)*2 <= maxDegree
}

func (nc nodeChildren) IsFull(maxDegree int) bool {
	return len(nc) == maxDegree
}

type nodeChild struct {
	Key   interface{}
	Value unsafe.Pointer
}

func syncKey(recordPath recordPath) {
	if leaf, recordIndex := recordPath.LocateRecord(); recordIndex == 0 && len(leaf.Records()) >= 1 {
		for i := len(recordPath) - 2; i >= 0; i-- {
			if recordPath[i].NodeChildIndex() >= 1 {
				nonLeaf := recordPath[i].NonLeaf()
				nonLeafChildIndex := recordPath[i].NodeChildIndex()
				nonLeaf.Children()[nonLeafChildIndex].Key = leaf.Records()[0].Key
				return
			}
		}
	}
}
