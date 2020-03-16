package bptree

import "unsafe"

type recordPath []recordPathComponent

func (rp *recordPath) Append(node unsafe.Pointer, nodeChildIndexOrRecordIndex int) {
	rp.insertComponent(recordPathComponent{node, nodeChildIndexOrRecordIndex}, len(*rp))
}

func (rp *recordPath) Prepend(node unsafe.Pointer, nodeChildIndexOrRecordIndex int) {
	rp.insertComponent(recordPathComponent{node, nodeChildIndexOrRecordIndex}, 0)
}

func (rp *recordPath) Unprepend() {
	rp.removeComponent(0)
}

func (rp recordPath) LocateRecord() (*leaf, int) {
	i := len(rp) - 1
	return rp[i].Leaf(), rp[i].RecordIndex()
}

func (rp *recordPath) insertComponent(component recordPathComponent, componentIndex int) {
	*rp = append(*rp, recordPathComponent{})
	copy((*rp)[componentIndex+1:], (*rp)[componentIndex:])
	(*rp)[componentIndex] = component
}

func (rp *recordPath) removeComponent(componentIndex int) recordPathComponent {
	component := (*rp)[componentIndex]
	copy((*rp)[componentIndex:], (*rp)[componentIndex+1:])
	(*rp)[len(*rp)-1] = recordPathComponent{}
	*rp = (*rp)[:len(*rp)-1]
	return component
}

type recordPathComponent struct {
	node                        unsafe.Pointer
	nodeChildIndexOrRecordIndex int
}

func (rpc *recordPathComponent) SetNonLeaf(nonLeaf *nonLeaf) {
	rpc.node = unsafe.Pointer(nonLeaf)
}

func (rpc *recordPathComponent) SetLeaf(leaf *leaf) {
	rpc.node = unsafe.Pointer(leaf)
}

func (rpc *recordPathComponent) SetNodeChildIndex(nodeChildIndex int) {
	rpc.nodeChildIndexOrRecordIndex = nodeChildIndex
}

func (rpc *recordPathComponent) SetRecordIndex(recordIndex int) {
	rpc.nodeChildIndexOrRecordIndex = recordIndex
}

func (rpc recordPathComponent) NonLeaf() *nonLeaf {
	return (*nonLeaf)(rpc.node)
}

func (rpc recordPathComponent) Leaf() *leaf {
	return (*leaf)(rpc.node)
}

func (rpc recordPathComponent) NodeChildIndex() int {
	return rpc.nodeChildIndexOrRecordIndex
}

func (rpc recordPathComponent) RecordIndex() int {
	return rpc.nodeChildIndexOrRecordIndex
}
