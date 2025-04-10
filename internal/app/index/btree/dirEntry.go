package btree

import "centauri/internal/app/types"

// Represents a directory entry in a B-tree index structure.
// Each directory entry serves as a navigation aid in the B-tree, containing two components:
// 1. A key value (dataval) that acts as a decision point for tree traversal.
// 2. A block number referring to a child page in the B-tree
type DirEntry struct {
	dataval  *types.Constant
	blocknum int
}

func NewDirEntry(dataval *types.Constant, blocknum int) *DirEntry {
	return &DirEntry{
		dataval:  dataval,
		blocknum: blocknum,
	}
}

// Returns the key value component of the directory entry.
// This value is used during B-tree traversal to determine which
// branch to follow.
// When searching for a key K:
// - If K < this entry's dataval, the search continues in the block to the left
// - If K >= this entry's dataval, the search continues in this entry's block
// The dataval is also used when inserting new directory entries to maintain
// the sorted order of entries within directory pages.
func (d *DirEntry) DataVal() *types.Constant {
	return d.dataval
}

// // This number identifies the specific block in the B-tree file that contains
// the child page (either another directory page or a leaf page) that this entry points to.
func (d *DirEntry) BlockNumber() int {
	return d.blocknum
}
