package bptree

import "errors"

// Iterator represents an iteration over records in a B+ Tree.
type Iterator interface {
	// IsAtEnd indicates if the iteration has no more records.
	IsAtEnd() (hasNoMoreRecords bool)

	// Record returns the current record in the iteration.
	Record() (key, value interface{})

	// Advance advances the iteration to the next record.
	Advance()
}

type forwardIterator struct{ iterator }

func (fi *forwardIterator) Init(firstLeaf *leaf, firstRecordIndex int, lastLeaf *leaf, lastRecordIndex int, isAtEnd bool) *forwardIterator {
	fi.init(firstLeaf, firstRecordIndex, lastLeaf, lastRecordIndex, isAtEnd)
	return fi
}

func (fi *forwardIterator) Advance() {
	fi.advance()

	if fi.isAtEnd {
		return
	}

	if fi.currentRecordIndex < len(fi.currentLeaf.Records)-1 {
		fi.currentRecordIndex++
	} else {
		fi.currentLeaf = fi.currentLeaf.Next
		fi.currentRecordIndex = 0
	}
}

type backwardIterator struct{ iterator }

func (bi *backwardIterator) Init(firstLeaf *leaf, firstRecordIndex int, lastLeaf *leaf, lastRecordIndex int, isAtEnd bool) *backwardIterator {
	bi.init(firstLeaf, firstRecordIndex, lastLeaf, lastRecordIndex, isAtEnd)
	return bi
}

func (bi *backwardIterator) Advance() {
	bi.advance()

	if bi.isAtEnd {
		return
	}

	if bi.currentRecordIndex >= 1 {
		bi.currentRecordIndex--
	} else {
		bi.currentLeaf = bi.currentLeaf.Prev
		bi.currentRecordIndex = len(bi.currentLeaf.Records) - 1
	}
}

type iterator struct {
	currentLeaf        *leaf
	currentRecordIndex int
	lastLeaf           *leaf
	lastRecordIndex    int
	isAtEnd            bool
}

func (i *iterator) Record() (interface{}, interface{}) {
	if i.isAtEnd {
		panic(errors.New("bptree: end of iteration"))
	}

	record := i.currentLeaf.Records[i.currentRecordIndex]
	return record.Key, record.Value
}

func (i *iterator) IsAtEnd() bool {
	return i.isAtEnd
}

func (i *iterator) init(firstLeaf *leaf, firstRecordIndex int, lastLeaf *leaf, lastRecordIndex int, isAtEnd bool) {
	i.currentLeaf = firstLeaf
	i.currentRecordIndex = firstRecordIndex
	i.lastLeaf = lastLeaf
	i.lastRecordIndex = lastRecordIndex
	i.isAtEnd = isAtEnd
}

func (i *iterator) advance() {
	if i.currentLeaf == i.lastLeaf && i.currentRecordIndex == i.lastRecordIndex {
		*i = iterator{isAtEnd: true}
	}
}
