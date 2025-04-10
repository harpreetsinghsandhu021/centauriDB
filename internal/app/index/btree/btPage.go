package btree

import (
	"centauri/internal/app/file"
	"centauri/internal/app/record"
	"centauri/internal/app/record/schema"
	"centauri/internal/app/tx"
	"centauri/internal/app/types"
)

// Represents common functionality for B-tree directory and leaf pages
// Both types of pages store records in sorted order when full
type BTPage struct {
	tx           *tx.Transaction
	currentBlock *file.BlockID
	layout       *record.Layout
}

func NewBTPage(tx *tx.Transaction, currentBlock *file.BlockID, layout *record.Layout) *BTPage {
	btp := &BTPage{
		tx:           tx,
		currentBlock: currentBlock,
		layout:       layout,
	}

	btp.tx.Pin(currentBlock)

	return btp
}

// Calculates the position where the first record having the specified search key should be,
// then returns the position before it. This method is used during B-tree traversal to find
// the appropriate subtree or record location.
func (p *BTPage) FindSlotBefore(searchKey *types.Constant) int {
	slot := 0
	// Iterate throught record until finding the first one that is greater than
	// or equal to search key
	for slot < p.GetNumRecs() && p.GetDataVal(slot).CompareTo(searchKey) < 0 {
		slot++
	}

	return slot - 1 // Return the position before the slot
}

// Unpins the page's buffer from memory, allowing it to be potentially flushed and removed from
// the buffer if needed.
func (p *BTPage) Close() {
	if p.currentBlock != nil {
		// Unpin the block since we're done with it
		p.tx.Unpin(p.currentBlock)

		// Clear the block reference to indicate that this page is no longer associated with
		// any block
		p.currentBlock = nil
	}
}

// Determines whether the page has room for additional records.
// This is used to decide when a page needs to be split.
func (p *BTPage) IsFull() bool {
	// Calculate the byte position needed for one more record
	// and check if it would exceed the block size
	return p.slotPos(p.GetNumRecs()+1) >= p.tx.BlockSize()
}

// Divides the page at the specified position by creating a new page.
// and transferring records starting at splitPos to the new page.
// This is a critical operation for B-tree growth.
// Parameters:
//   - splitPos: the position where the split should occur
//   - flag:      the initial value for the flag field of the new page
//
// Returns:
//   - the Reference to the nre block created by the split
func (p *BTPage) Split(splitPos int, flag int) *file.BlockID {
	newBlock := p.AppendNew(flag)                  // Create a new block with the specified flag
	newPage := NewBTPage(p.tx, newBlock, p.layout) // Create a BTPage wrapper for the new block
	p.transferRecs(splitPos, newPage)              // Transfer records starting from splitPos to the new page
	newPage.SetFlag(flag)                          // Ensure the flag is set correctly
	newPage.Close()                                // Clean up by unpinning the new page

	return newBlock
}

// Returns the data value of the record at the specified slot
// In a leaf page, this is the indexed value
// In a directory page, this is search key value
func (p *BTPage) GetDataVal(slot int) *types.Constant {
	return p.getVal(slot, "dataval")
}

// Returns the value of the page's flag field
// The flag indicates the type or state of the page (e.g leaf or directory)
func (p *BTPage) GetFlag() int {
	val, _ := p.tx.GetInt(*p.currentBlock, 0)
	return int(val)
}

// Updates the page's flag field to the specified value.
// This might change the type or state of the page
func (p *BTPage) SetFlag(val int) {
	p.tx.SetInt(*p.currentBlock, 0, val, true)
}

// Creates a new block at the end of the B-tree file with the specified flag value. This is
// used during page splits and tree growth.
func (p *BTPage) AppendNew(flag int) *file.BlockID {
	// Append a new block to the file
	block, _ := p.tx.Append(p.currentBlock.FileName())
	// Pin the block in the memory
	p.tx.Pin(&block)
	// Initialize the block with the specified flag
	p.Format(&block, flag)

	return &block
}

// Initializes a block with the specified flag, setting record count to 0 and creating empty
// records throughtout the page. This prepares a newly created block for use in the B-tree.
func (p *BTPage) Format(block *file.BlockID, flag int) {
	p.tx.SetInt(*block, 0, flag, false)
	p.tx.SetInt(*block, 4, 0, false)
	recSize := p.layout.SlotSize()

	// Intialize all possible record slots with default values
	for pos := 2 * 4; pos+recSize <= p.tx.BlockSize(); pos += recSize {
		p.makeDefaultRecord(block, pos)
	}
}

// Inits a record slot with default values (zeros or empty strings). This creates a blank
// slate for records that will be inseted later
func (p *BTPage) makeDefaultRecord(block *file.BlockID, pos int) {
	// Loop through each field in the schema
	for _, fieldName := range p.layout.Schema().Fields() {
		offset := p.layout.Offset(fieldName)

		// Init the field based on its type
		if p.layout.Schema().DataType(fieldName) == schema.INTEGER {
			// Init integer fields to 0
			p.tx.SetInt(*block, pos+offset, 0, false)
		} else {
			// Init string fields to empty string
			p.tx.SetString(*block, pos+offset, "", false)
		}
	}
}

// Retrieves the block number stored in the index record at the specified slot.
// This method is used primarily by directory pages to navigate to child nodes.
func (p *BTPage) GetChildNum(slot int) int {
	return p.getInt(slot, "block")
}

// Inserts a directory entry at the specified slot.
// Directory entries contain a data value (search key) and a block number pointing to a
// subtree containing values related to that key.
// Parameters:
//   - slot:     the slot position where the new entry should be inserted
//   - val:      the data value to be stored
//   - blockNum: the block number pointing to the relevant subtree
func (p *BTPage) InsertDir(slot int, val *types.Constant, blockNum int) {
	// Make room for the new entry
	p.insert(slot)
	// Set the data value (search key)
	p.setVal(slot, "dataval", val)
	// Set the block number reference
	p.setInt(slot, "block", blockNum)
}

// Retrieves the record ID (RID) stored in the specified leaf index record.
// In leaf pages, each entry contains a data value and a reference to the actual
// data record in the database.
func (p *BTPage) GetDataRid(slot int) *types.RID {
	//  Create a RID from the block number and slot stored in the index entry
	return types.NewRID(p.getInt(slot, "block"), p.getInt(slot, "id"))
}

// Adds a leaf index record at the specified slot.
// Leaf entries contain a data value and a reference (RID) to the actual data record
// in the database.
func (p *BTPage) InsertLeaf(slot int, val *types.Constant, rid *types.RID) {
	// Make room for the new entry
	p.insert(slot)

	// Set the data value
	p.setVal(slot, "dataval", val)

	// Store the RID components (block number and slot)
	p.setInt(slot, "block", rid.BlockNumber())
	p.setInt(slot, "id", rid.Slot())
}

// Removes the index record at the specified slot and shifts subsequent records to fill the gap.
func (p *BTPage) Delete(slot int) {
	// Shift all records after the deleted one to fill the gap
	for i := slot + 1; i < p.GetNumRecs(); i++ {
		p.copyRecord(i, i-1)
	}

	// Decrement the record count
	p.SetNumRecs(p.GetNumRecs() - 1)
}

// Returns the number of index records currently stored in this page
// This count is stored at a fixed position in the block header.
func (p *BTPage) GetNumRecs() int {
	val, _ := p.tx.GetInt(*p.currentBlock, 4)
	return int(val)
}

// Private helper methods

// Retrieves an integer value from a specific field in the record at the given slot.
func (p *BTPage) getInt(slot int, fieldName string) int {
	pos := p.fldPos(slot, fieldName)

	val, _ := p.tx.GetInt(*p.currentBlock, pos)
	return int(val)
}

// Retrieves a string value from a specific field in the record at the given slot.
func (p *BTPage) getString(slot int, fldName string) string {
	pos := p.fldPos(slot, fldName)
	val, _ := p.tx.GetString(*p.currentBlock, pos)

	return val
}

// Retrieves a value as a constant from a specific field in the record at the given slot.
// It handles type conversion based on the field's defined type in the schema.
func (p *BTPage) getVal(slot int, fldName string) *types.Constant {
	fieldType := p.layout.Schema().DataType(fldName)

	// Retrieve and convert the value based on its type
	if fieldType == schema.INTEGER {
		return types.NewConstantInt(p.getInt(slot, fldName))
	} else {
		return types.NewConstantString(p.getString(slot, fldName))
	}
}

// Stores an integer value in a specific field in the record at the given slot.
func (p *BTPage) setInt(slot int, fldName string, val int) {
	pos := p.fldPos(slot, fldName)

	p.tx.SetInt(*p.currentBlock, pos, val, true)
}

// Stores a string value in a specific field in the record at the given slot.
func (p *BTPage) setString(slot int, fldName string, val string) {
	pos := p.fldPos(slot, fldName)

	p.tx.SetString(*p.currentBlock, pos, val, true)
}

// Stores a Constant value in a specific field in the record at the given slot.
// It handles type conversion based on the field's defined type in the schema.
func (p *BTPage) setVal(slot int, fldName string, val *types.Constant) {
	fieldType := p.layout.Schema().DataType(fldName)

	if fieldType == schema.INTEGER {
		p.setInt(slot, fldName, *val.AsInt())
	} else {
		p.setString(slot, fldName, *val.AsString())
	}
}

// Updates the record count stored in the page header.
// n is the number of new records.
func (p *BTPage) SetNumRecs(n int) {
	p.tx.SetInt(*p.currentBlock, 4, n, true)
}

// Creates space for a new record at the specified slot position.
// It shifts existing records and updates the records count.
func (p *BTPage) insert(slot int) {
	// Shift all records at or after the slot position to make room
	for i := p.GetNumRecs(); i > slot; i-- {
		p.copyRecord(i-1, i)
	}

	// Increment the record count
	p.SetNumRecs(p.GetNumRecs() + 1)
}

// Copies all field values from one record slot to another.
func (p *BTPage) copyRecord(from, to int) {
	// Get the schema to determine which fields to copy
	sch := p.layout.Schema()

	for _, fieldName := range sch.Fields() {
		p.setVal(to, fieldName, p.getVal(from, fieldName))
	}
}

// Moves records from this page to the destination page.
// Used during page splits to redistribute records.
// Parameters:
//   - slot: the starting slot from which to transfer records
//   - dest: the destination page to receive the records
func (p *BTPage) transferRecs(slot int, dest *BTPage) {
	destSlot := 0

	// Continue transferring while there are records left to move
	for slot < p.GetNumRecs() {
		dest.insert(destSlot) // Make space in the destination page

		// Copy each field from the source to the destination
		sch := p.layout.Schema()
		for _, fieldName := range sch.Fields() {
			dest.setVal(destSlot, fieldName, p.getVal(slot, fieldName))
		}

		// Delete the record from the source page
		p.Delete(slot)

		destSlot++

		// NOTE: slot does'nt increment because Delete shits records down
	}
}

// Calculates the byte position of a specific field within a record at the given slot.
func (p *BTPage) fldPos(slot int, fldName string) int {
	offset := p.layout.Offset(fldName) // Get the offset

	return p.slotPos(slot) + offset // Add the offset to the start position of the record
}

// Calculates the byte position where a record slot begins in the block.
func (p *BTPage) slotPos(slot int) int {
	// Calculate the size of each record slot
	slotSize := p.layout.SlotSize()

	// The record area starts after the flag and record count(both integers)
	// Each slot is located at an offset based on its slot number
	return 4 + 4 + (slot * slotSize)
}
