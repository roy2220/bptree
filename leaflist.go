package bptree

type leafList struct {
	tail *leaf
	head *leaf
}

func (ll *leafList) Init(leaf *leaf) *leafList {
	leaf.Prev = leaf
	leaf.Next = leaf
	ll.tail = leaf
	ll.head = leaf
	return ll
}

func (ll *leafList) InsertLeafAfter(leaf, otherLeaf *leaf) {
	leaf.insert(otherLeaf, otherLeaf.Next)

	if otherLeaf == ll.tail {
		ll.tail = leaf
	}
}

func (ll *leafList) RemoveLeaf(leaf *leaf) {
	leaf.remove()

	if leaf == ll.tail {
		ll.tail = leaf.Prev
	} else if leaf == ll.head {
		ll.head = leaf.Next
	}
}

func (ll *leafList) Tail() *leaf {
	return ll.tail
}

func (ll *leafList) Head() *leaf {
	return ll.head
}

func (l *leaf) insert(prev, next *leaf) {
	l.Prev = prev
	prev.Next = l
	l.Next = next
	next.Prev = l
}

func (l *leaf) remove() {
	prev, next := l.Prev, l.Next
	prev.Next = next
	next.Prev = prev
}
