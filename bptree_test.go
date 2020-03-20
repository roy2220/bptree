package bptree_test

import (
	"bytes"
	"io/ioutil"
	"math/rand"
	"os"
	"sort"
	"strings"
	"testing"

	"github.com/roy2220/bptree"
	"github.com/stretchr/testify/assert"
)

var Keywords []string
var SortedKeywordIndexes []int
var SortedKeywordRIndexes []int

func TestBPTreeAddRecord(t *testing.T) {
	bpt := MakeBPTree(t, 5)

	for i, k := range Keywords {
		j, ok := bpt.AddRecord(k, nil)

		if assert.False(t, ok, "%v %v", i, k) {
			assert.Equal(t, i, j)
		}
	}

	v, ok := bpt.AddRecord(bptree.KeyMin, nil)

	if assert.False(t, ok) {
		assert.Equal(t, SortedKeywordIndexes[0], v)
	}

	v, ok = bpt.AddRecord(bptree.KeyMax, nil)

	if assert.False(t, ok) {
		assert.Equal(t, SortedKeywordIndexes[len(Keywords)-1], v)
	}
}

func TestBPTreeUpdateRecord(t *testing.T) {
	bpt := MakeBPTree(t, 5)

	for i, k := range Keywords {
		j, ok := bpt.UpdateRecord(k, i)

		if assert.True(t, ok, "%v %v", i, k) {
			assert.Equal(t, i, j)
		}
	}

	v, ok := bpt.UpdateRecord(bptree.KeyMin, nil)

	if assert.True(t, ok) {
		assert.Equal(t, SortedKeywordIndexes[0], v)
	}

	v, ok = bpt.UpdateRecord(bptree.KeyMax, nil)

	if assert.True(t, ok) {
		assert.Equal(t, SortedKeywordIndexes[len(Keywords)-1], v)
	}
}

func TestBPTreeDeleteRecord(t *testing.T) {
	bpt := MakeBPTree(t, 5)
	l, h := 0, len(Keywords)-1

	for l < len(Keywords)/8 {
		j := SortedKeywordIndexes[l]
		l++
		k := Keywords[j]
		j2, ok := bpt.DeleteRecord(k)
		if assert.True(t, ok) {
			assert.Equal(t, j, j2)
		}
	}

	for h > len(Keywords)/8*7 {
		j := SortedKeywordIndexes[h]
		h--
		k := Keywords[j]
		j2, ok := bpt.DeleteRecord(k)
		if assert.True(t, ok) {
			assert.Equal(t, j, j2)
		}
	}

	for h-l+1 > len(Keywords)/2 {
		v, ok := bpt.DeleteRecord(bptree.KeyMin)

		if assert.True(t, ok) {
			assert.Equal(t, SortedKeywordIndexes[l], v)
		}

		v, ok = bpt.DeleteRecord(bptree.KeyMax)

		if assert.True(t, ok) {
			assert.Equal(t, SortedKeywordIndexes[h], v)
		}

		l++
		h--
	}

	for i, k := range Keywords {
		j, ok := bpt.DeleteRecord(k)

		if r := SortedKeywordRIndexes[i]; r >= l && r <= h {
			if assert.True(t, ok, "%v %v", i, k) {
				assert.Equal(t, i, j)
			}

			continue
		}

		assert.False(t, ok, "%v %v", i, k)
	}

	assert.True(t, bpt.IsEmpty())
	assert.Equal(t, 1, bpt.Height())
	b := bytes.NewBuffer(nil)
	bpt.Fprint(b)
	t.Logf("fprint: %s", b.String())
	assert.Equal(t, "", b.String())
}

func TestBPTreeHasRecord(t *testing.T) {
	bpt := MakeBPTree(t, 5)

	for _, i := range SortedKeywordIndexes {
		k := Keywords[i]
		j, ok := bpt.HasRecord(k)

		if assert.True(t, ok, "%v %v", i, k) {
			assert.Equal(t, i, j)
		}
	}
}

func TestBPTreeSearch(t *testing.T) {
	bpt := MakeBPTree(t, 5)
	minI := SortedKeywordIndexes[0]
	maxI := SortedKeywordIndexes[len(Keywords)-1]

	for _, tmp := range [][2]interface{}{
		{bptree.KeyMin, bptree.KeyMax},
		{Keywords[minI], bptree.KeyMax},
		{bptree.KeyMin, Keywords[maxI]},
		{Keywords[minI], Keywords[maxI]},
	} {
		minKey, maxKey := tmp[0], tmp[1]

		{
			i := 0

			for it := bpt.SearchForward(minKey, maxKey); !it.IsAtEnd(); it.Advance() {
				j := SortedKeywordIndexes[i]
				k := Keywords[j]
				k2, j2 := it.Record()

				if assert.Equal(t, k, k2) {
					assert.Equal(t, j, j2)
				}

				i++
			}

			assert.Equal(t, len(Keywords), i)
			it := bpt.SearchForward(maxKey, minKey)
			assert.True(t, it.IsAtEnd())
		}

		{
			i := len(Keywords) - 1

			for it := bpt.SearchBackward(minKey, maxKey); !it.IsAtEnd(); it.Advance() {
				j := SortedKeywordIndexes[i]
				k := Keywords[j]
				k2, j2 := it.Record()

				if assert.Equal(t, k, k2) {
					assert.Equal(t, j, j2)
				}

				i--
			}

			assert.Equal(t, -1, i)
			it := bpt.SearchForward(maxKey, minKey)
			assert.True(t, it.IsAtEnd())
		}
	}

	for _, tmp := range [][3]interface{}{
		{bptree.KeyMin, bptree.KeyMin, minI},
		{bptree.KeyMin, Keywords[minI], minI},
		{Keywords[minI], bptree.KeyMin, minI},
		{Keywords[minI], Keywords[minI], minI},
		{bptree.KeyMax, bptree.KeyMax, maxI},
		{bptree.KeyMax, Keywords[maxI], maxI},
		{Keywords[maxI], bptree.KeyMax, maxI},
		{Keywords[maxI], Keywords[maxI], maxI},
	} {
		minKey, maxKey, i := tmp[0], tmp[1], tmp[2]
		it := bpt.SearchBackward(minKey, maxKey)

		if assert.True(t, !it.IsAtEnd()) {
			_, j := it.Record()
			assert.Equal(t, i, j)
			it.Advance()
			assert.True(t, it.IsAtEnd())
		}
	}

	{
		bpt := new(bptree.BPTree).Init(5, func(key1, key2 interface{}) int64 {
			return int64(strings.Compare(key1.(string), key2.(string)))
		})

		it := bpt.SearchForward(bptree.KeyMin, bptree.KeyMax)
		assert.True(t, it.IsAtEnd())
		it = bpt.SearchForward(bptree.KeyMax, bptree.KeyMin)
		assert.True(t, it.IsAtEnd())

		bpt.AddRecord("aa", nil)

		it = bpt.SearchForward(bptree.KeyMin, bptree.KeyMax)
		assert.True(t, !it.IsAtEnd())
		it = bpt.SearchForward(bptree.KeyMax, bptree.KeyMin)
		assert.True(t, !it.IsAtEnd())

		bpt.AddRecord("dd", nil)

		it = bpt.SearchForward(bptree.KeyMin, bptree.KeyMax)
		assert.True(t, !it.IsAtEnd())
		it = bpt.SearchForward(bptree.KeyMax, bptree.KeyMin)
		assert.True(t, it.IsAtEnd())

		bpt.AddRecord("cc", nil)
		bpt.AddRecord("bb", nil)
		bpt.AddRecord("ii", nil)
		bpt.AddRecord("ff", nil)
		bpt.AddRecord("hh", nil)
		bpt.AddRecord("kk", nil)
		bpt.AddRecord("ee", nil)
		bpt.AddRecord("jj", nil)
		bpt.AddRecord("gg", nil)

		b := bytes.NewBuffer(nil)
		bpt.Fprint(b)
		t.Logf("\n%s", b.String())

		it = bpt.SearchForward("", "b")
		if assert.True(t, !it.IsAtEnd()) {
			k, _ := it.Record()
			assert.Equal(t, "aa", k)
			it.Advance()
			assert.True(t, it.IsAtEnd())
		}

		it = bpt.SearchForward("ddd", "f")
		if assert.True(t, !it.IsAtEnd()) {
			k, _ := it.Record()
			assert.Equal(t, "ee", k)
			it.Advance()
			assert.True(t, it.IsAtEnd())
		}

		it = bpt.SearchForward("hhh", "j")
		if assert.True(t, !it.IsAtEnd()) {
			k, _ := it.Record()
			assert.Equal(t, "ii", k)
			it.Advance()
			assert.True(t, it.IsAtEnd())
		}

		it = bpt.SearchForward("jjj", "l")
		if assert.True(t, !it.IsAtEnd()) {
			k, _ := it.Record()
			assert.Equal(t, "kk", k)
			it.Advance()
			assert.True(t, it.IsAtEnd())
		}

		it = bpt.SearchForward(bptree.KeyMin, "a ")
		assert.True(t, it.IsAtEnd())

		it = bpt.SearchForward("l", bptree.KeyMax)
		assert.True(t, it.IsAtEnd())

		it = bpt.SearchForward("a", "a ")
		assert.True(t, it.IsAtEnd())

		it = bpt.SearchForward("l", "l ")
		assert.True(t, it.IsAtEnd())

		it = bpt.SearchForward("g", "g ")
		assert.True(t, it.IsAtEnd())
	}
}

func MakeBPTree(t *testing.T, maxDegree int) *bptree.BPTree {
	bpt := new(bptree.BPTree).Init(maxDegree, func(key1, key2 interface{}) int64 {
		return int64(strings.Compare(key1.(string), key2.(string)))
	})

	deletedKeywordIndexes := make(map[int]struct{}, len(Keywords)/2)

	for i, k := range Keywords {
		_, ok := bpt.AddRecord(k, i)

		if !assert.True(t, ok, "%v %v", i, k) {
			t.FailNow()
		}

		// b := bytes.NewBuffer(nil)
		// bpt.Fprint(b)
		// t.Logf("after add: %v\n%s", k, b.String())
		j := rand.Intn(i*2 + 1)

		if j <= i {
			if _, ok := deletedKeywordIndexes[j]; !ok {
				_, ok2 := bpt.DeleteRecord(Keywords[j])

				if !assert.True(t, ok2, "%v %v", j, Keywords[j]) {
					t.FailNow()
				}

				// b := bytes.NewBuffer(nil)
				// bpt.Fprint(b)
				// t.Logf("after del: %v\n%s", k, b.String())
				deletedKeywordIndexes[j] = struct{}{}
			}
		}
	}

	for j := range deletedKeywordIndexes {
		k := Keywords[j]
		_, ok := bpt.AddRecord(k, j)

		if !assert.True(t, ok, "%v %v", j, k) {
			t.FailNow()
		}

		// b := bytes.NewBuffer(nil)
		// bpt.Fprint(b)
		// t.Logf("after add: %v\n%s", k, b.String())
	}

	t.Logf("b+ tree height: %d", bpt.Height())
	return bpt
}

func TestMain(m *testing.M) {
	data, err := ioutil.ReadFile("./test/data/bitquark-subdomains-top100000.txt")

	if err != nil {
		panic(err)
	}

	Keywords = strings.Split(string(data), "\n")
	Keywords = Keywords[:len(Keywords)-1]
	// Keywords = Keywords[:20]
	SortedKeywordIndexes = make([]int, len(Keywords))

	for i := range Keywords {
		SortedKeywordIndexes[i] = i
	}

	sort.Slice(SortedKeywordIndexes, func(i, j int) bool {
		return Keywords[SortedKeywordIndexes[i]] < Keywords[SortedKeywordIndexes[j]]
	})

	SortedKeywordRIndexes = make([]int, len(SortedKeywordIndexes))

	for r, i := range SortedKeywordIndexes {
		SortedKeywordRIndexes[i] = r
	}

	os.Exit(m.Run())
}
