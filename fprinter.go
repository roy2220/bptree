package bptree

import (
	"fmt"
	"io"
)

// Fprint dumps the B+ tree as plain text for debugging purposes.
func (bpt *BPTree) Fprint(writer io.Writer) error {
	return bpt.Walk((&fprinter{writer, "", "\n"}).Fprint)
}

type fprinter struct {
	Writer  io.Writer
	Prefix  string
	NewLine string
}

func (f *fprinter) Fprint(nodeAccessor NodeAccessor) error {
	if nodeAccessor.IsLeaf() {
		n := nodeAccessor.NumberOfKeys()

		for i := 0; i < n; i++ {
			key, value := nodeAccessor.GetKey(i), nodeAccessor.GetValue(i)
			var err error

			switch i {
			case 0:
				if n == 1 {
					_, err = fmt.Fprintf(f.Writer, "%s──● %v=%v", f.Prefix, key, value)
				} else {
					_, err = fmt.Fprintf(f.Writer, "%s┬─● %v=%v", f.Prefix, key, value)
				}
			case n - 1:
				_, err = fmt.Fprintf(f.Writer, "%s└─● %v=%v", f.NewLine, key, value)
			default:
				_, err = fmt.Fprintf(f.Writer, "%s├─● %v=%v", f.NewLine, key, value)
			}

			if err != nil {
				return err
			}
		}
	} else {
		prefix, newLine := f.Prefix, f.NewLine
		f.Prefix, f.NewLine = prefix+"┬─", newLine+"│ "

		if err := nodeAccessor.AccessChild(f.Fprint, 0); err != nil {
			return err
		}

		n := nodeAccessor.NumberOfKeys()

		for i := 0; i < n; i++ {
			key := nodeAccessor.GetKey(i)

			if _, err := fmt.Fprintf(f.Writer, "%s├─● %v", newLine, key); err != nil {
				return err
			}

			if i == n-1 {
				f.Prefix, f.NewLine = newLine+"└─", newLine+"  "
			} else {
				f.Prefix, f.NewLine = newLine+"├─", newLine+"│ "
			}

			if err := nodeAccessor.AccessChild(f.Fprint, i+1); err != nil {
				return err
			}
		}

		f.Prefix, f.NewLine = prefix, newLine
	}

	return nil
}
