// Package bptree implements an in-memory B+ tree.
package bptree

import (
	"errors"
	"fmt"
	"io"
	"unsafe"
)

const (
	// KeyMin is at present the minimum key in the B+ tree.
	KeyMin = keyMinMax(-1)

	// KeyMax is at present the maximum key in the B+ tree.
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
	if maxDegree < 3 {
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
// it adds the record then returns true, otherwise if returns
// false and the present value of the record.
func (bpt *BPTree) AddRecord(key, value interface{}) (interface{}, bool) {
	recordPath, ok := bpt.findRecord(key)

	if ok {
		leaf, recordIndex := recordPath.LocateRecord()
		return leaf.Records[recordIndex].Value, false
	}

	bpt.insertRecord(record{key, value}, recordPath)
	return nil, true
}

// UpdateRecord replaces the value of a record with the given
// key in the B+ tree to the given one.
// If a record with an identical key exists in the B+ tree,
// it updates the record then returns true and the replaced
// value of the record, otherwise if returns flase.
func (bpt *BPTree) UpdateRecord(key, value interface{}) (interface{}, bool) {
	if recordPath, ok := bpt.findRecord(key); ok {
		leaf, recordIndex := recordPath.LocateRecord()
		leaf.Records[recordIndex].Value, value = value, leaf.Records[recordIndex].Value
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
		leaf.Records[recordIndex].Value, value = value, leaf.Records[recordIndex].Value
		return value, false
	}

	bpt.insertRecord(record{key, value}, recordPath)
	return nil, true
}

// DeleteRecord deletes a record with the given key in the
// B+ tree.
// If a record with an identical key exists in the B+ tree,
// it deletes the record then returns true and the removed
// value of the record, otherwise if returns flase.
func (bpt *BPTree) DeleteRecord(key interface{}) (interface{}, bool) {
	if recordPath, ok := bpt.findRecord(key); ok {
		leaf, recordIndex := recordPath.LocateRecord()
		value := leaf.Records[recordIndex].Value
		bpt.removeRecord(recordPath)
		return value, true
	}

	return nil, false
}

// HasRecord checks whether a record with the given key
// in the B+ tree.
// If a record with an identical key exists in the B+ tree,
// it returns true and the present value of the record,
// otherwise if returns flase.
func (bpt *BPTree) HasRecord(key interface{}) (interface{}, bool) {
	if recordPath, ok := bpt.findRecord(key); ok {
		leaf, recordIndex := recordPath.LocateRecord()
		return leaf.Records[recordIndex].Value, true
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

// Fprint dumps the B+ tree as plain text for debugging purposes.
func (bpt *BPTree) Fprint(writer io.Writer) error {
	return bpt.doFprint(writer, bpt.root, 1, "", "\n")
}

// IsEmpty indicates whether the B+ tree is empty.
func (bpt *BPTree) IsEmpty() bool {
	return bpt.height == 1 && len((*leaf)(bpt.root).Records) == 0
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
			i, ok := leaf.Records.LocateRecord(key, bpt.keyComparer)
			recordPath.Append(node, i)
			return recordPath, ok
		}

		nonLeaf := (*nonLeaf)(node)
		i, ok := nonLeaf.Children.LocateNodeChild(key, bpt.keyComparer)

		if !ok {
			i--
		}

		recordPath.Append(node, i)
		node = nonLeaf.Children[i].Value
	}
}

func (bpt *BPTree) insertRecord(record record, recordPath recordPath) {
	for i := 0; ; i++ {
		n := len(recordPath)

		if i >= n {
			break
		}

		if i == n-1 {
			bpt.trySplitLeaf(&recordPath, &i)
		} else {
			bpt.trySplitNonLeaf(&recordPath, &i)
		}
	}

	leaf, recordIndex := recordPath.LocateRecord()
	leaf.Records.InsertRecord(record, recordIndex)
	trySyncKey(recordPath)
}

func (bpt *BPTree) removeRecord(recordPath recordPath) {
	for i := 1; ; i++ {
		n := len(recordPath)

		if i >= n {
			break
		}

		if i == n-1 {
			bpt.tryMergeLeaf(&recordPath, &i)
		} else {
			bpt.tryMergeNonLeaf(&recordPath, &i)
		}
	}

	leaf, recordIndex := recordPath.LocateRecord()
	leaf.Records.RemoveRecord(recordIndex)
	trySyncKey(recordPath)
}

func (bpt *BPTree) trySplitLeaf(recordPath *recordPath, i *int) {
	leaf := (*recordPath)[*i].Leaf()
	recordIndex := (*recordPath)[*i].RecordIndex()

	if leaf.Records.IsFull(bpt.maxDegree) {
		numberOfRecords := bpt.maxDegree / 2

		if *i == 0 {
			bpt.increaseHeight()
			recordPath.Prepend(bpt.root, 0)
			*i++
		}

		leafParent := (*recordPath)[*i-1].NonLeaf()
		leafIndex := (*recordPath)[*i-1].NodeChildIndex()
		leafSibling := leaf.Split(numberOfRecords, leafParent, leafIndex+1)
		bpt.leafList.InsertLeafAfter(leafSibling, leaf)

		if recordIndex >= numberOfRecords {
			(*recordPath)[*i].SetLeaf(leafSibling)
			(*recordPath)[*i].SetRecordIndex(recordIndex - numberOfRecords)
			(*recordPath)[*i-1].SetNodeChildIndex(leafIndex + 1)
		}
	}
}

func (bpt *BPTree) trySplitNonLeaf(recordPath *recordPath, i *int) {
	nonLeaf := (*recordPath)[*i].NonLeaf()
	nonLeafChildIndex := (*recordPath)[*i].NodeChildIndex()

	if nonLeaf.Children.IsFull(bpt.maxDegree) {
		numberOfNonLeafChildren := (bpt.maxDegree - 1) / 2

		if *i == 0 {
			bpt.increaseHeight()
			recordPath.Prepend(bpt.root, 0)
			*i++
		}

		nonLeafParent := (*recordPath)[*i-1].NonLeaf()
		nonLeafIndex := (*recordPath)[*i-1].NodeChildIndex()
		nonLeafSibling := nonLeaf.Split(numberOfNonLeafChildren, nonLeafParent, nonLeafIndex+1)

		if nonLeafChildIndex >= numberOfNonLeafChildren {
			(*recordPath)[*i].SetNonLeaf(nonLeafSibling)
			(*recordPath)[*i].SetNodeChildIndex(nonLeafChildIndex - numberOfNonLeafChildren)
			(*recordPath)[*i-1].SetNodeChildIndex(nonLeafIndex + 1)
		}
	}
}

func (bpt *BPTree) tryMergeLeaf(recordPath *recordPath, i *int) {
	leaf1 := (*recordPath)[*i].Leaf()
	recordIndex := (*recordPath)[*i].RecordIndex()

	if leaf1.Records.IsSparse(bpt.maxDegree) {
		leafParent := (*recordPath)[*i-1].NonLeaf()
		leafIndex := (*recordPath)[*i-1].NodeChildIndex()

		if leafIndex < len(leafParent.Children)-1 {
			if (*leaf)(leafParent.Children[leafIndex+1].Value).Records.IsSparse(bpt.maxDegree) {
				bpt.leafList.RemoveLeaf(leaf1.MergeRight(leafParent, leafIndex))
			} else {
				leaf1.StealRecordRight(leafParent, leafIndex)
			}
		} else {
			if (*leaf)(leafParent.Children[leafIndex-1].Value).Records.IsSparse(bpt.maxDegree) {
				numberOfRecords := len(leaf1.Records)
				leafSibling := leaf1.MergeToLeft(leafParent, leafIndex)
				bpt.leafList.RemoveLeaf(leaf1)
				(*recordPath)[*i].SetLeaf(leafSibling)
				(*recordPath)[*i].SetRecordIndex(len(leafSibling.Records) - (numberOfRecords - recordIndex))
				(*recordPath)[*i-1].SetNodeChildIndex(leafIndex - 1)
			} else {
				leaf1.StealRecordLeft(leafParent, leafIndex)
				(*recordPath)[*i].SetRecordIndex(recordIndex + 1)
			}
		}

		if len(leafParent.Children) == 1 {
			bpt.decreaseHeight()
			recordPath.Unprepend()
			*i--
		}
	}
}

func (bpt *BPTree) tryMergeNonLeaf(recordPath *recordPath, i *int) {
	nonLeaf1 := (*recordPath)[*i].NonLeaf()
	nonLeafChildIndex := (*recordPath)[*i].NodeChildIndex()

	if nonLeaf1.Children.IsSparse(bpt.maxDegree) {
		nonLeafParent := (*recordPath)[*i-1].NonLeaf()
		nonLeafIndex := (*recordPath)[*i-1].NodeChildIndex()

		if nonLeafIndex < len(nonLeafParent.Children)-1 {
			if (*nonLeaf)(nonLeafParent.Children[nonLeafIndex+1].Value).Children.IsSparse(bpt.maxDegree) {
				nonLeaf1.MergeRight(nonLeafParent, nonLeafIndex)
			} else {
				nonLeaf1.StealChildRight(nonLeafParent, nonLeafIndex)
			}
		} else {
			if (*nonLeaf)(nonLeafParent.Children[nonLeafIndex-1].Value).Children.IsSparse(bpt.maxDegree) {
				numberOfNonLeafChildren := len(nonLeaf1.Children)
				nonLeafSibling := nonLeaf1.MergeToLeft(nonLeafParent, nonLeafIndex)
				(*recordPath)[*i].SetNonLeaf(nonLeafSibling)
				(*recordPath)[*i].SetNodeChildIndex(len(nonLeafSibling.Children) - (numberOfNonLeafChildren - nonLeafChildIndex))
				(*recordPath)[*i-1].SetNodeChildIndex(nonLeafIndex - 1)
			} else {
				nonLeaf1.StealChildLeft(nonLeafParent, nonLeafIndex)
				(*recordPath)[*i].SetNodeChildIndex(nonLeafChildIndex + 1)
			}
		}

		if len(nonLeafParent.Children) == 1 {
			bpt.decreaseHeight()
			recordPath.Unprepend()
			*i--
		}
	}
}

func (bpt *BPTree) increaseHeight() {
	bpt.root = unsafe.Pointer(&nonLeaf{Children: []nodeChild{{nil, bpt.root}}})
	bpt.height++
}

func (bpt *BPTree) decreaseHeight() {
	bpt.root = (*nonLeaf)(bpt.root).Children[0].Value
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
		if minRecordIndex == len(minLeaf.Records) {
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
		minKey = minLeaf.Records[minRecordIndex].Key

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
		//	maxRecordIndex = len(maxLeaf.Records) - 1
		// } else {
		maxRecordIndex--
		// }
	}

	if ok2 || !ok4 {
		maxKey = maxLeaf.Records[maxRecordIndex].Key
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

func (bpt *BPTree) doFprint(writer io.Writer, node unsafe.Pointer, nodeDepth int, prefix, newLine string) error {
	if nodeDepth == bpt.height {
		leaf := (*leaf)(node)

		for i, record := range leaf.Records {
			var err error

			switch i {
			case 0:
				if len(leaf.Records) == 1 {
					_, err = fmt.Fprintf(writer, "%s──● %v", prefix, record)
				} else {
					_, err = fmt.Fprintf(writer, "%s┬─● %v", prefix, record)
				}
			case len(leaf.Records) - 1:
				_, err = fmt.Fprintf(writer, "%s└─● %v", newLine, record)
			default:
				_, err = fmt.Fprintf(writer, "%s├─● %v", newLine, record)
			}

			if err != nil {
				return err
			}
		}
	} else {
		nonLeaf := (*nonLeaf)(node)

		if err := bpt.doFprint(writer, nonLeaf.Children[0].Value, nodeDepth+1, prefix+"┬─", newLine+"│ "); err != nil {
			return err
		}

		for i, nodeChild := range nonLeaf.Children[1:] {
			if _, err := fmt.Fprintf(writer, "%s├─● %v", newLine, nodeChild.Key); err != nil {
				return err
			}

			var prefix2, newLine2 string

			if i == len(nonLeaf.Children[1:])-1 {
				prefix2, newLine2 = "└─", "  "
			} else {
				prefix2, newLine2 = "├─", "│ "
			}

			if err := bpt.doFprint(writer, nodeChild.Value, nodeDepth+1, newLine+prefix2, newLine+newLine2); err != nil {
				return err
			}
		}
	}

	return nil
}

// KeyComparer compares two keys and returns an integer
// with a value < 0 means the key 1 is less than the key 2,
// with a value == 0 means the key 1 is equal to the key 2,
// with a value > 0 means the key 1 is greater to the key 2.
type KeyComparer func(key1, key2 interface{}) (delta int64)

type keyMinMax int

type leaf struct {
	Records records
	Prev    *leaf
	Next    *leaf
}

func (l *leaf) Split(numberOfRecords int, parent *nonLeaf, index int) *leaf {
	sibling := leaf{
		Records: make([]record, len(l.Records)-numberOfRecords),
	}

	copy(sibling.Records, l.Records[numberOfRecords:])
	l.Records.Truncate(numberOfRecords)
	parent.Children.InsertNodeChild(nodeChild{sibling.Records[0].Key, unsafe.Pointer(&sibling)}, index)
	return &sibling
}

func (l *leaf) MergeToLeft(parent *nonLeaf, index int) *leaf {
	sibling := (*leaf)(parent.Children[index-1].Value)
	parent.Children.RemoveNodeChild(index)
	sibling.Records = append(sibling.Records, l.Records...)
	l.Records.Truncate(0)
	return sibling
}

func (l *leaf) MergeRight(parent *nonLeaf, index int) *leaf {
	sibling := (*leaf)(parent.Children[index+1].Value)
	parent.Children.RemoveNodeChild(index + 1)
	l.Records = append(l.Records, sibling.Records...)
	sibling.Records.Truncate(0)
	return sibling
}

func (l *leaf) StealRecordLeft(parent *nonLeaf, index int) {
	sibling := (*leaf)(parent.Children[index-1].Value)
	record := sibling.Records.RemoveRecord(len(sibling.Records) - 1)
	l.Records.InsertRecord(record, 0)
	parent.Children[index].Key = record.Key
}

func (l *leaf) StealRecordRight(parent *nonLeaf, index int) {
	sibling := (*leaf)(parent.Children[index+1].Value)
	record := sibling.Records.RemoveRecord(0)
	parent.Children[index+1].Key = sibling.Records[0].Key
	l.Records.InsertRecord(record, len(l.Records))
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
	Children nodeChildren
}

func (nl *nonLeaf) Split(numberOfChildren int, parent *nonLeaf, index int) *nonLeaf {
	sibling := nonLeaf{
		Children: make([]nodeChild, len(nl.Children)-numberOfChildren),
	}

	copy(sibling.Children, nl.Children[numberOfChildren:])
	key := sibling.Children[0].Key
	sibling.Children[0].Key = nil
	nl.Children.Truncate(numberOfChildren)
	parent.Children.InsertNodeChild(nodeChild{key, unsafe.Pointer(&sibling)}, index)
	return &sibling
}

func (nl *nonLeaf) MergeToLeft(parent *nonLeaf, index int) *nonLeaf {
	sibling := (*nonLeaf)(parent.Children[index-1].Value)
	nl.Children[0].Key = parent.Children[index].Key
	parent.Children.RemoveNodeChild(index)
	sibling.Children = append(sibling.Children, nl.Children...)
	nl.Children.Truncate(0)
	return sibling
}

func (nl *nonLeaf) MergeRight(parent *nonLeaf, index int) *nonLeaf {
	sibling := (*nonLeaf)(parent.Children[index+1].Value)
	sibling.Children[0].Key = parent.Children[index+1].Key
	parent.Children.RemoveNodeChild(index + 1)
	nl.Children = append(nl.Children, sibling.Children...)
	sibling.Children.Truncate(0)
	return sibling
}

func (nl *nonLeaf) StealChildLeft(parent *nonLeaf, index int) {
	sibling := (*nonLeaf)(parent.Children[index-1].Value)
	child := sibling.Children.RemoveNodeChild(len(sibling.Children) - 1)
	nl.Children[0].Key = parent.Children[index].Key
	parent.Children[index].Key = child.Key
	child.Key = nil
	nl.Children.InsertNodeChild(child, 0)
}

func (nl *nonLeaf) StealChildRight(parent *nonLeaf, index int) {
	sibling := (*nonLeaf)(parent.Children[index+1].Value)
	child := sibling.Children.RemoveNodeChild(0)
	child.Key = parent.Children[index+1].Key
	parent.Children[index+1].Key = sibling.Children[0].Key
	sibling.Children[0].Key = nil
	nl.Children.InsertNodeChild(child, len(nl.Children))
}

type nodeChildren []nodeChild

func (nc nodeChildren) LocateNodeChild(key interface{}, keyComparer KeyComparer) (int, bool) {
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

func (nc *nodeChildren) InsertNodeChild(nodeChild1 nodeChild, nodeChildIndex int) {
	*nc = append(*nc, nodeChild{})
	copy((*nc)[nodeChildIndex+1:], (*nc)[nodeChildIndex:])
	(*nc)[nodeChildIndex] = nodeChild1
}

func (nc *nodeChildren) RemoveNodeChild(nodeChildIndex int) nodeChild {
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
	return len(nc)*2 <= maxDegree-1
}

func (nc nodeChildren) IsFull(maxDegree int) bool {
	return len(nc) == maxDegree
}

type nodeChild struct {
	Key   interface{}
	Value unsafe.Pointer
}

func trySyncKey(recordPath recordPath) {
	if leaf, recordIndex := recordPath.LocateRecord(); recordIndex == 0 && len(leaf.Records) >= 1 {
		for i := len(recordPath) - 2; i >= 0; i-- {
			if recordPath[i].NodeChildIndex() >= 1 {
				nonLeaf := recordPath[i].NonLeaf()
				nonLeafChildIndex := recordPath[i].NodeChildIndex()
				nonLeaf.Children[nonLeafChildIndex].Key = leaf.Records[0].Key
				return
			}
		}
	}
}
