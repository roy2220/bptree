# bptree

[![GoDoc](https://godoc.org/github.com/roy2220/bptree?status.svg)](https://godoc.org/github.com/roy2220/bptree) [![Build Status](https://travis-ci.com/roy2220/bptree.svg?branch=master)](https://travis-ci.com/roy2220/bptree) [![Coverage Status](https://codecov.io/gh/roy2220/bptree/branch/master/graph/badge.svg)](https://codecov.io/gh/roy2220/bptree)

An implementation of in-memory B+ tree in pure Go

## Example

```go
package main

import (
        "fmt"

        "github.com/roy2220/bptree"
)

func main() {
        // initialize
        bpt := new(bptree.BPTree).Init(
                5, // maximum degree
                func(key1, key2 interface{}) int64 { // key comparer
                        return int64(strings.Compare(key1.(string), key2.(string)))
                },
        )

        // add records
        bpt.AddRecord("a", 1)
        bpt.AddRecord("b", 2)
        bpt.AddRecord("c", 3)
        bpt.AddRecord("d", 4)
        bpt.AddRecord("e", 5)
        bpt.AddRecord("f", 6)

        // get values
        fmt.Println("---1---")
        v, ok := bpt.HasRecord("c")
        fmt.Println(v, ok)
        v, ok = bpt.HasRecord("g")
        fmt.Println(v, ok)

        // delete records
        fmt.Println("---2---")
        v, ok = bpt.DeleteRecord("c")
        fmt.Println(v, ok)
        v, ok = bpt.DeleteRecord("g")
        fmt.Println(v, ok)

        // traverse
        fmt.Println("---3---")
        for it := bpt.SearchForward(bptree.KeyMin, bptree.KeyMax); !it.IsAtEnd(); it.Advance() {
                k, v := it.Record()
                fmt.Println(k, v)
        }

        // search for records with keys between "a"..."c"
        fmt.Println("---4---")
        for it := bpt.SearchForward("a", "c"); !it.IsAtEnd(); it.Advance() {
                k, v := it.Record()
                fmt.Println(k, v)
        }

        // search for records with keys between "c"..."a"
        fmt.Println("---5---")
        for it := bpt.SearchBackward("a", "c"); !it.IsAtEnd(); it.Advance() {
                k, v := it.Record()
                fmt.Println(k, v)
        }

        // get record with key >= "bbb"
        fmt.Println("---6---")
        if it := bpt.SearchForward("bbb", bptree.KeyMax); !it.IsAtEnd() {
                k, v := it.Record()
                fmt.Println(k, v)
        }

        // get record with key <= "eee"
        fmt.Println("---7---")
        if it := bpt.SearchBackward(bptree.KeyMin, "eee"); !it.IsAtEnd() {
                k, v := it.Record()
                fmt.Println(k, v)
        }

        // get record with minimum key
        fmt.Println("---8---")
        if !bpt.IsEmpty() {
                k, v := bpt.SearchForward(bptree.KeyMin, bptree.KeyMin).Record()
                fmt.Println(k, v)
        }

        // get record with maximum key
        fmt.Println("---9---")
        if !bpt.IsEmpty() {
                k, v := bpt.SearchBackward(bptree.KeyMax, bptree.KeyMax).Record()
                fmt.Println(k, v)
        }
        // Output:
        // ---1---
        // 3 true
        // <nil> false
        // ---2---
        // 3 true
        // <nil> false
        // ---3---
        // a 1
        // b 2
        // d 4
        // e 5
        // f 6
        // ---4---
        // a 1
        // b 2
        // ---5---
        // b 2
        // a 1
        // ---6---
        // d 4
        // ---7---
        // e 5
        // ---8---
        // a 1
        // ---9---
        // f 6
}
```
