package bptree

import "unsafe"

// Walk walks the B+ tree by calling the given walker function
// for each node in the tree.
func (bpt *BPTree) Walk(walker Walker) error {
	return bpt.doWalk(walker, bpt.root, 1)
}

func (bpt *BPTree) doWalk(walker Walker, node unsafe.Pointer, nodeDepth int) error {
	var err error

	if nodeDepth == bpt.height {
		err = walker(&leafAccessor{bpt, (*leaf)(node)})
	} else {
		err = walker(&nonLeafAccessor{bpt, (*nonLeaf)(node), nodeDepth})
	}

	return err
}

// Walker is the type of the function called while walking
// a B+ tree.
type Walker func(nodeAccessor NodeAccessor) (err error)

// NodeAccessor presents a node accessor for walking a B+
// tree.
type NodeAccessor interface {
	IsLeaf() bool
	NumberOfKeys() int
	GetKey(keyIndex int) (key interface{})
	GetValue(keyIndex int) (value interface{})
	AccessChild(walker Walker, childIndex int) (err error)
}

type leafAccessor struct {
	bpt *BPTree
	l   *leaf
}

func (li *leafAccessor) IsLeaf() bool {
	return true
}

func (li *leafAccessor) NumberOfKeys() int {
	return len(li.l.Records())
}

func (li *leafAccessor) GetKey(keyIndex int) interface{} {
	return li.l.Records()[keyIndex].Key
}

func (li *leafAccessor) GetValue(keyIndex int) interface{} {
	return li.l.Records()[keyIndex].Value
}

func (li *leafAccessor) AccessChild(Walker, int) (_ error) { return }

type nonLeafAccessor struct {
	bpt   *BPTree
	nl    *nonLeaf
	depth int
}

func (nli *nonLeafAccessor) IsLeaf() bool {
	return false
}

func (nli *nonLeafAccessor) NumberOfKeys() int {
	return len(nli.nl.Children()) - 1
}

func (nli *nonLeafAccessor) GetKey(keyIndex int) interface{} {
	return nli.nl.Children()[keyIndex+1].Key
}

func (nli *nonLeafAccessor) AccessChild(walker Walker, childIndex int) error {
	child := nli.nl.Children()[childIndex].Value
	nli.bpt.doWalk(walker, child, nli.depth+1)
	return nil
}

func (nli *nonLeafAccessor) GetValue(int) (_ interface{}) { return }
