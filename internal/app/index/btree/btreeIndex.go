package btree

import (
	"centauri/internal/app/file"
	"centauri/internal/app/record"
	"centauri/internal/app/record/schema"
	"centauri/internal/app/tx"
	"centauri/internal/app/types"
	"math"
)

// Implements the Index interface using a B-tree structure.
// A B-tree is a balanced tree data structure that maintains sorted data
// and allows searches, insertions, and deletions in logarithmic time.
// This implementation consists of a directory structure pointing to leaf pages
// that contain the actual index entries.
// This implementation follows a classic B-tree architecture with several key components:
// TWO-LAYER FILE STRUCTURE
// Directory Layer:
//   - Stored in a file named "{idxname}dir"
//   - Contains heirarchy of directory pages organizing the index
//   - Root is always at block 0
//   - Each entry contains a key value and a pointer to the child node
//
// Leaf Layer:
//   - Stored in a file named "{idxname}leaf"
//   - Contains actual index entries (key-RID pairs)
//   - Leaf pages are linked for efficient range queries
//   - All data records are stored at the leaf level
//
// This two layer approach provides a balanced, scalable structure that grows
// gracefully as the index size increases.
// PAGE TYPES AND ORGANIZATION
// Directory Pages:
// - flag value > 0 indicates level in directory hierarchy
// - Each entry contains:
//   - `dataval`: key value serving as decision point for traversal
//   - `block`: block number of child page (directory or leaf)
//
// Leaf Pages:
// Flag value -1 identifies leaf pages
// Each entry contains:
//   - `dataval`: Indexed key value
//   - `block` and `id`: components of the RID pointing to the actual data record
//
// Overflow Pages:
// - Special extension of leaf pages when many entries have the same key
// - Connected by the flag field which points to the next overflow block
type BTreeIndex struct {
	tx         *tx.Transaction
	dirLayout  *record.Layout
	leafLayout *record.Layout
	leaftbl    string // name of the leaf table file
	leaf       *BTreeLeaf
	rootBlock  *file.BlockID
}

func NewBTreeIndex(tx *tx.Transaction, idxname string, leafLayout *record.Layout) *BTreeIndex {
	idx := &BTreeIndex{
		tx:         tx,
		leafLayout: leafLayout,
		leaftbl:    idxname + "leaf",
	}

	// Handle leaf pages initialization
	// If the leaf file does'nt exist or is empty, create initial leaf block
	if size, _ := tx.Size(idx.leaftbl); size == 0 {
		// Append a new block to the leaf file
		block, _ := tx.Append(idx.leaftbl)
		// Create a BTPage for the new leaf block
		node := NewBTPage(tx, &block, leafLayout)
		// Format the block as a leaf page (flag = -1 for leaf pages)
		node.Format(&block, -1)
	}

	// Handle directory pages initialization
	// Create schema for directory entries
	dirsch := schema.NewSchema()

	// Directory entries need "block" and "dataval" fields
	// with same types as in the leaf schema
	dirsch.Add("block", leafLayout.Schema())
	dirsch.Add("dataval", leafLayout.Schema())

	// Name of the directory table file
	dirtbl := idxname + "dir"

	// Create layout for directory pages
	idx.dirLayout = record.NewLayout(dirsch)

	// Root is always at block 0
	idx.rootBlock = file.NewBlockID(dirtbl, 0)

	// If the directory file does'nt exist or is empty, create initial directory structure
	if size, _ := tx.Size(dirtbl); size == 0 {
		// Append a new block to the directory file (will be block 0)
		tx.Append(dirtbl)
		// Create a BTPage for the root block
		node := NewBTPage(tx, idx.rootBlock, idx.dirLayout)
		// Format the block as a directory page(flag=0 means directory pointing to leaves)
		node.Format(idx.rootBlock, 0)

		// Create a minimum value based on the dataval field type
		// This sentinel value ensures all searches start from the leftmost leaf
		var minval *types.Constant
		fieldType := dirsch.DataType("dataval")
		if fieldType == schema.INTEGER {
			minval = types.NewConstantInt(math.MinInt32)
		} else {
			minval = types.NewConstantString("")
		}

		// Insert initial directory entry pointing to the first leaf block
		// This entry has the minimum possible value and points to block 0 of the leaf file.
		node.InsertDir(0, minval, 0)

		// Close the root page
		node.Close()
	}

	return idx
}

// Positions the index at the beginning of the entries having the specified search key.
// After this method is called, the next() method can be used to iterate through the matching entries.
// The method traverses the directory to find the appropriate leaf block, then positions that leaf block
// at the first occurence (if any) of the search key.
func (idx *BTreeIndex) BeforeFirst(searchKey *types.Constant) {
	// Close any previously opened leaf page
	idx.Close()
	// Open the root directory
	root := NewBTreeDir(idx.tx, idx.rootBlock, idx.dirLayout)
	// Search the directory structure to find the appropriate leaf block number
	blockNum := root.Search(searchKey)
	// Close the root directory
	root.Close()
	// Create a block ID for the leaf page
	leafBlock := file.NewBlockID(idx.leaftbl, blockNum)
	// Open the leaf page and position it for the given search key
	idx.leaf = NewBtreeLeaf(idx.tx, leafBlock, idx.leafLayout, searchKey)
}

// Moves to the next leaf entry matching the search key specified in the most recent beforeFirst call.
// This allows iterating through all entries with a particular key.
func (idx *BTreeIndex) Next() bool {
	return idx.leaf.Next()
}

// Returns the RID from the current leaf entry.
// This RID points to the actual data record in the database that is index by the current entry.
func (idx *BTreeIndex) GetDataRid() *types.RID {
	return idx.leaf.GetDataRid()
}

// Adds a new entry to the index with the specified key value and RID. This method:
// 1. Navigates to the appropriate leaf page
// 2. Inserts the entry
// 3. Handles any page splits that occur, potentially updating the directory structure
func (idx *BTreeIndex) Insert(dataval *types.Constant, datarid *types.RID) {
	// First navigate to the appropriate leaf page for this key
	idx.BeforeFirst(dataval)
	// Insert the entry into the leaf, which may cause a split
	// If a split occurs, e will contain info about the new leaf
	e := idx.leaf.Insert(datarid)
	// Close the leaf page now that insertion is complete
	idx.leaf.Close()
	// If no split occured, we're done
	if e == nil {
		return
	}

	// A leaf split occured, so we need to update the directory
	// Open the root directory
	root := NewBTreeDir(idx.tx, idx.rootBlock, idx.dirLayout)
	// Insert the new directory entry, which may cause directory splits
	e2 := root.Insert(e)
	// If a dir split propagated all the way to the root, we need to create a new root
	if e2 != nil {
		root.MakeNewRoot(e2)
	}

	root.Close()
}

// Removes the specified entry from the index.
// The method navigates to the appropriate leaf page and deletes
// the entry matching both the key value and RID.
func (idx *BTreeIndex) Delete(dataval *types.Constant, datarid *types.RID) {
	idx.BeforeFirst(dataval)
	idx.leaf.Delete(datarid)
	idx.leaf.Close()
}

// Releases resources by closing the current leaf page if it's open.
func (idx *BTreeIndex) Close() {
	if idx.leaf != nil {
		idx.leaf.Close()
		idx.leaf = nil
	}
}

// Estimates the number of block accesses required to find all index records with a
// particular search key. This is a static utility method used for query optimization.
// Parameters:
//   - numblocks: The number of blocks in the B-tree directory
//   - rpb: The number of records (entries) per block
func SearchCost(numBlocks int, rpb int) int {
	// Cost is 1 (for the leaf access) plus the height of the directory tree
	return 1 + int(math.Log(float64(numBlocks))/math.Log(float64(rpb)))
}
