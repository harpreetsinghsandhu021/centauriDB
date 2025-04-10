package btree

import (
	"centauri/internal/app/file"
	"centauri/internal/app/record"
	"centauri/internal/app/tx"
	"centauri/internal/app/types"
)

// Encapsulates the functionality of a B-tree directory(non-leaf) node.
// Directory nodes contain entries that point to child nodes in the B-tree structure.
// Each entry consists of a key value and a block number reference to a child node.
// The directory nodes form the internal structure of the B-tree and facilitate efficient
// traversal and search operations.
type BTreeDir struct {
	tx       *tx.Transaction
	layout   *record.Layout
	contents *BTPage
	fileName string
}

func NewBTreeDir(tx *tx.Transaction, block *file.BlockID, layout *record.Layout) *BTreeDir {
	return &BTreeDir{
		tx:       tx,
		layout:   layout,
		contents: NewBTPage(tx, block, layout),
		fileName: block.FileName(),
	}
}

// Releases the resources used by this directory node.
// This should be called when operations with the directory are complete
// to ensure proper resource management.
func (d *BTreeDir) Close() {
	d.contents.Close()
}

// Navigates through the B-tree structure to find the block number of the leaf page
// that should contain the specified search key. This method starts at the current
// directory node and traverses down the tree until reaching a leaf node.
func (d *BTreeDir) Search(searchKey *types.Constant) int {
	// Find the initial child block that might contain the search key
	childBlock := d.findChildBlock(searchKey)

	// Continue traversing down the tree until reaching a leaf level
	// The flag in a directory page indicates its level in the tree
	// (flag > 0 means it's a directory page, not a leaf)
	for d.contents.GetFlag() > 0 {
		// Close the current page since we're moving to another
		d.contents.Close()
		// Open the child block
		d.contents = NewBTPage(d.tx, childBlock, d.layout)
		// Find the next child block to follow
		childBlock = d.findChildBlock(searchKey)
	}

	// Return the block number of the leaf page
	return childBlock.Number()
}

// Creates a new root block for the B-tree when the current root splits.
// The new root will have two children:
//   - the old root(with its contents moved to a new block)
//   - the specified directory entry (pointing to another block)
//
// Since the root must always be in block 0 of the file, the contents of the old root
// must get transferred to a new block.
// Parameters:
//   - e: The directory entry to be added as the second child of the new root (the first child will
//
// be the old root's contents)
func (d *BTreeDir) MakeNewRoot(e *DirEntry) {
	// Get the smallest key in the current root
	firstVal := d.contents.GetDataVal(0)

	// Get the current level of the root
	level := d.contents.GetFlag()

	// Split the root, moving all of its contents to a new block
	// The "0" parameters means to split at position 0, essentially transferring
	// all records to the new block
	newBlock := d.contents.Split(0, level)

	// Create a directory entry for the old root's contents
	oldroot := NewDirEntry(firstVal, newBlock.Number())

	// Insert the entry for the old root into the now-empty root block
	d.insertEntry(oldroot)

	// Insert the provided directory entry as the second child
	d.insertEntry(e)

	// Set the flag to indicate the new root is one level higher than before
	// This maintains the property that the root is always at the highest level
	d.contents.SetFlag(level + 1)
}

// Adds a new directory entry into the B-tree directory structure.
// If this directory is at level 0(just above the leaf level), then this entry
// is inserted directly. Otherwise, the insertion is recursively propagated to
// the appropriate child node. Directory nodes might split during insertion, in which
// case the split information is propagated upward.
// This recursive approach ensures that the B-tree remains balanced as entries are added
// Parameters:
//   - e: The directory entry to be inserted into the tree structure
//
// Returns:
//   - A new directory entry if this node split during insertion, or nil otherwise
func (d *BTreeDir) Insert(e *DirEntry) *DirEntry {
	// If this directory is just above the leaf level (flag=0)
	// insert the entry directly
	if d.contents.GetFlag() == 0 {
		return d.insertEntry(e)
	}

	// For higher-level directories, find the appropriate child where the entry should be
	// inserted
	childBlock := d.findChildBlock(e.DataVal())

	// Create a directory object for the child block
	child := NewBTreeDir(d.tx, childBlock, d.layout)

	// Recursively insert the entry into the child
	myentry := child.Insert(e)

	// Close the child directory
	child.Close()

	// If the child split (myentry is not nil), insert the returned entry
	if myentry != nil {
		return d.insertEntry(myentry)
	}

	return nil
}

// Adds a directory entry to the current directory page.
// If the page becomes full and splits, this method returns a directory entry
// for the new page that was created during the split.
// The split promotes the middle key to the parent level, which is a fundamental
// operation in maintaning B-tree balance.
// Parameters:
//   - e: The directory entry to insert
//
// Returns:
//   - A new directory entry if this node split, or nil if no split occured
func (d *BTreeDir) insertEntry(e *DirEntry) *DirEntry {
	// Find the correct position to insert the new entry
	// The +1 is needed because findSlotBefore returns the position
	// before where the entry should go
	newSlot := 1 + d.contents.FindSlotBefore(e.DataVal())

	// Insert the directory entry at the calculated position
	d.contents.InsertDir(newSlot, e.DataVal(), e.BlockNumber())

	// If the page is'nt full after insertion, we're done
	if !d.contents.IsFull() {
		return nil
	}

	// The page is full, so split it

	// Get the current level of this directory
	level := d.contents.GetFlag()

	// Find the middle position for a balanced split
	splitPos := d.contents.GetNumRecs() / 2

	// Get the key value at the split position
	// This will be promoted to the parent level
	splitVal := d.contents.GetDataVal(splitPos)

	// Split the page, creating a new block containing entries from splitpos onwards
	newBlock := d.contents.Split(splitPos, level)

	// Create and return a directory entry for the new block
	// This entry will be inserted into the parent directory
	return NewDirEntry(splitVal, newBlock.Number())
}

// Determines which child block to follow when searching for a specific
// key in the B-tree structure.
// This method implements a critical part of the B-tree search algorithmn:
// given a key, it finds the appropriate branch to follow in the directory.
func (d *BTreeDir) findChildBlock(searchKey *types.Constant) *file.BlockID {
	// Find the largest slot whose key is less than or equal to the search key
	slot := d.contents.FindSlotBefore(searchKey)

	// If the next key exactly matches the search key, use that slot instead
	// This ensures we follow the correct path when the key exists in the tree
	if slot+1 < d.contents.GetNumRecs() && d.contents.GetDataVal(slot+1).Equals(searchKey) {
		slot++
	}

	// Get the block number of the child at determined slot
	blockNum := d.contents.GetChildNum(slot)

	return file.NewBlockID(d.fileName, blockNum)
}
