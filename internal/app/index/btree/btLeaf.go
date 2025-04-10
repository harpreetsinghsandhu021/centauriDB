package btree

import (
	"centauri/internal/app/file"
	"centauri/internal/app/record"
	"centauri/internal/app/tx"
	"centauri/internal/app/types"
)

// Represents a leaf node in a B-tree index structure.
// It holds the contents of a B-tree leaf block and provides methods
// for navigating through records with a given search key, as well as
// inserting and deleting records.
type BTreeLeaf struct {
	tx          *tx.Transaction
	layout      *record.Layout
	searchKey   *types.Constant
	contents    *BTPage
	currentSlot int
	fileName    string
}

func NewBtreeLeaf(tx *tx.Transaction, block *file.BlockID, layout *record.Layout, searchKey *types.Constant) *BTreeLeaf {
	leaf := &BTreeLeaf{
		tx:        tx,
		layout:    layout,
		searchKey: searchKey,
		fileName:  block.FileName(),
	}

	leaf.contents = NewBTPage(tx, block, layout)

	// Position the cursor just before the first occurence of the search key
	leaf.currentSlot = leaf.contents.FindSlotBefore(searchKey)

	return leaf
}

// Release the resources used by this leaf node.
// This should be called when operations with the leaf are complete
// to ensure proper resource management.
func (l *BTreeLeaf) Close() {
	l.contents.Close()
}

// Moves to the next leaf record having the previously specified search key.
// It handles navigation across overflow blocks if necessary.
func (l *BTreeLeaf) Next() bool {
	// Move to the next slot
	l.currentSlot++

	// Check if we've reached the end of the current page
	if l.currentSlot >= l.contents.GetNumRecs() {
		// Try moving to an overflow block if one exists
		return l.tryOverflow()
	} else if l.contents.GetDataVal(l.currentSlot).Equals(l.searchKey) {
		// Found a matching record
		return true
	} else {
		// No more matching records in this page, try overflow
		return l.tryOverflow()
	}
}

// Returns the RID of the current leaf record.
// This RID points to the actual data record in the database that is
// indexed by this leaf entry.
func (l *BTreeLeaf) GetDataRid() *types.RID {
	return l.contents.GetDataRid(l.currentSlot)
}

// Removes the leaf record that points to the specified data record.
// It scans through records with the search key until finding one with
// a matching dataRID, then deletes that record.
func (l *BTreeLeaf) Delete(datarid *types.RID) {
	// Scan through all records with the search key
	for l.Next() {
		// If the current record points to the target data record
		if l.GetDataRid().Equals(datarid) {
			// Delete this index entry
			l.contents.Delete(l.currentSlot)
			return
		}
	}
}

// Adds a new leaf record for the specified data record using the previously-specified
// search key. If the leaf page becomes full, it may split or create an overflow page depending
// on the distribution of keys.
func (l *BTreeLeaf) Insert(datarid *types.RID) *DirEntry {
	// Case 1: Handling a special insertion scenario where:
	// - Either this is the very first insertion into an empty leaf page.
	// - OR the new entry needs to go before all existing entries in the leaf
	if l.contents.GetFlag() >= 0 && // for a empty page, the flag is set to a value >= 0
		l.contents.GetDataVal(0).CompareTo(l.searchKey) > 0 { // Checks if the first key in the leaf is greater than the new key being inserted
		// Get the value of the first record in the page
		firstVal := l.contents.GetDataVal(0)
		// Split the page at position 0(moving all existing records to a new page)
		newBlock := l.contents.Split(0, l.contents.GetFlag())
		// Reset current position to beginning
		l.currentSlot = 0
		// Set flag to indicate this is no longer an overflow block
		l.contents.SetFlag(-1)
		// Insert the new record at the beginning
		l.contents.InsertLeaf(l.currentSlot, l.searchKey, datarid)

		// Return a new directory entry pointing to the new block, with the original first value as key
		return NewDirEntry(firstVal, newBlock.Number())
	}

	// Normal insertion increment position and insert the record
	l.currentSlot++
	l.contents.InsertLeaf(l.currentSlot, l.searchKey, datarid)

	// If the page is'nt full after insertion, we're done
	if !l.contents.IsFull() {
		return nil
	}

	// The page is full, so we need to either split or create an overflow page

	// Get the first and last keys in the page
	firstKey := l.contents.GetDataVal(0)
	lastKey := l.contents.GetDataVal(l.contents.GetNumRecs() - 1)

	// Case 2: All records have the same key - create an overflow block
	if lastKey.Equals(firstKey) {
		// Create an overflow block to hold all but the first record
		newBlock := l.contents.Split(1, l.contents.GetFlag())
		// Set the current block's flag to point to the overflow block
		l.contents.SetFlag(newBlock.Number())

		// No directory entry needed for overflow blocks
		return nil
	}

	// Case 3: Normal split with different keys in the page
	splitPos := l.contents.GetNumRecs() / 2
	splitKey := l.contents.GetDataVal(splitPos)

	if splitKey.Equals(firstKey) {
		// If the split would occur in the middle of records with the same key,
		// adjust the split position to be after all records with that key
		for splitPos < l.contents.GetNumRecs() && l.contents.GetDataVal(splitPos).Equals(splitKey) {
			splitPos++
		}

		splitKey = l.contents.GetDataVal(splitPos)
	} else {
		// If the split would occur in the middle of records with the same key,
		// adjust the split position to be at the first record with that key
		for splitPos > 0 && l.contents.GetDataVal(splitPos-1).Equals(splitKey) {
			splitPos--
		}
	}

	// Create a new block with the records at and after the split position
	newBlock := l.contents.Split(splitPos, -1) // -1 flag means not an overflow block

	return NewDirEntry(splitKey, newBlock.Number())
}

// Attempts to move to an overflow block if one exists.
// Overflow blocks are used when a leaf page contains too many records with the same search key value.
func (l *BTreeLeaf) tryOverflow() bool {
	// Check if there could be an overflow block:
	// 1. We must be looking for the same key as the first record
	// 2. The flag must be non-negative (indicating an overflow block exists)
	firstKey := l.contents.GetDataVal(0)
	flag := l.contents.GetFlag()

	if !l.searchKey.Equals(firstKey) || flag < 0 {
		// No overflow block exists for our search key
		return false
	}

	// Close the current page since we're moving to another
	l.contents.Close()

	// Open the overflow block
	nextBlock := file.NewBlockID(l.fileName, flag)
	l.contents = NewBTPage(l.tx, nextBlock, l.layout)

	// Start at the beginning of the overflow block
	l.currentSlot = 0

	// Successfully moved to the overflow block
	return true
}
