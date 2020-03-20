// +build js,wasm

package main

import (
	"bytes"
	"fmt"
	"syscall/js"

	"github.com/roy2220/bptree"
)

func BPTree_Init(_ js.Value, args []js.Value) interface{} {
	maxDegree := args[0].Int()

	bpTree.Init(maxDegree, func(key1, key2 interface{}) int64 {
		d := key1.(float64) - key2.(float64)

		switch {
		case d < 0:
			return -1
		case d == 0:
			return 0
		default:
			return 1
		}
	})

	return nil
}

func BPTree_AddKey(_ js.Value, args []js.Value) interface{} {
	key := args[0].Float()
	_, ok := bpTree.AddRecord(key, nil)
	return ok
}

func BPTree_DeleteKey(_ js.Value, args []js.Value) interface{} {
	key := args[0].Float()
	_, ok := bpTree.DeleteRecord(key)
	return ok
}

func BPTree_HasKey(_ js.Value, args []js.Value) interface{} {
	key := args[0].Float()
	_, ok := bpTree.HasRecord(key)
	return ok
}

func BPTree_FindMax(_ js.Value, _ []js.Value) interface{} {
	if it := bpTree.SearchBackward(bptree.KeyMax, bptree.KeyMax); !it.IsAtEnd() {
		key, _ := it.Record()
		return key
	}

	return nil
}

func BPTree_Dump(_ js.Value, _ []js.Value) interface{} {
	buffer := bytes.NewBuffer(nil)
	bpTree.Walk(makeBPTreeDumper(buffer))
	return buffer.String()
}

func BPTree_String(_ js.Value, _ []js.Value) interface{} {
	buffer := bytes.NewBuffer(nil)
	bpTree.Fprint(buffer)
	return buffer.String()
}

var bpTree bptree.BPTree

func makeBPTreeDumper(buffer *bytes.Buffer) bptree.Walker {
	var bpTreeDumper bptree.Walker

	bpTreeDumper = func(bpTreeNodeAccessor bptree.NodeAccessor) error {
		buffer.WriteByte('[')

		if bpTreeNodeAccessor.IsLeaf() {
			n := bpTreeNodeAccessor.NumberOfKeys()

			for i := 0; i < n; i++ {
				key := bpTreeNodeAccessor.GetKey(i)
				fmt.Fprintf(buffer, "%v", key)

				if i < n-1 {
					buffer.WriteByte(',')
				}
			}
		} else {
			bpTreeNodeAccessor.AccessChild(bpTreeDumper, 0)
			n := bpTreeNodeAccessor.NumberOfKeys()

			for i := 0; i < n; i++ {
				key := bpTreeNodeAccessor.GetKey(i)
				fmt.Fprintf(buffer, ",%v,", key)
				bpTreeNodeAccessor.AccessChild(bpTreeDumper, i+1)
			}
		}

		buffer.WriteByte(']')
		return nil
	}

	return bpTreeDumper
}

func main() {
	js.Global().Set("BPTree", make(map[string]interface{}))
	module := js.Global().Get("BPTree")
	module.Set("init", js.FuncOf(BPTree_Init))
	module.Set("addKey", js.FuncOf(BPTree_AddKey))
	module.Set("deleteKey", js.FuncOf(BPTree_DeleteKey))
	module.Set("hasKey", js.FuncOf(BPTree_HasKey))
	module.Set("findMax", js.FuncOf(BPTree_FindMax))
	module.Set("dump", js.FuncOf(BPTree_Dump))
	module.Set("string", js.FuncOf(BPTree_String))
	select {}
}
