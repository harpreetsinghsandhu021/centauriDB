package record

import (
	"centauri/internal/app/file"
	"centauri/internal/app/tx"
)

// Provides the abstraction for scanning and manipulating records in a table
// It implements the UpdateScan interface which allows both reading and modifying records
// The scanner maintains a current position in the table and provides methods to navigate through records
type TableScan struct {
	tx          *tx.Transaction
	layout      *Layout
	rp          *RecordPage
	filename    string
	currentSlot int
}

func NewTableScan(tx *tx.Transaction, tableName string, layout *Layout) *TableScan {
	ts := &TableScan{
		tx:          tx,
		layout:      layout,
		filename:    tableName + ".tbl",
		currentSlot: -1,
	}

	// Check if the table file exists and has any blocks
	size, _ := tx.Size(ts.filename)

	// For empty tables, create the first block
	// For existing tables, position at the first block
	if size == 0 {
		ts.moveToNewBlock()
	} else {
		ts.moveToBlock(0)
	}

	return ts
}

// Positions the scan before the first record
// This allows for a fresh scan of the table from the beginning
func (ts *TableScan) BeforeFirst() {
	ts.moveToBlock(0)
}

// Moves to the next record in the table
// Returns false if there are no more records
func (ts *TableScan) Next() bool {
	// Try to move to next slot in the current block
	ts.currentSlot = ts.rp.nextAfter(ts.currentSlot)

	// If no more slots in current block
	if ts.currentSlot < 0 {
		// Check if we're at the last block
		if ts.atLastBlock() {
			return false
		}
		// Move to next block and try again
		ts.moveToBlock(ts.rp.Block().Number() + 1)
	}
	return true
}

// Retrieves an integer value from the current record
func (ts *TableScan) GetInt(fieldname string) int {
	return ts.rp.GetInt(ts.currentSlot, fieldname)
}

// Retrieves a string value from the current record
func (ts *TableScan) GetString(fieldname string) string {
	return ts.rp.GetString(ts.currentSlot, fieldname)
}

// Releases any resources held by the scanner
// This primarily involves unpinning the current block
func (ts *TableScan) Close() {
	if ts.rp != nil {
		ts.tx.Unpin(ts.rp.Block())
	}
}

// Positions the scanner at the specified block number
func (ts *TableScan) moveToBlock(blockNum int) {
	ts.Close() // Release current block if any
	block := file.NewBlockID(ts.filename, blockNum)
	ts.rp = NewRecordPage(ts.tx, block, ts.layout)
	ts.currentSlot = -1 // Reset position within new block
}

// Appends a new block to the table and positions the scanner there
// This is used when we need to expand the table
func (ts *TableScan) moveToNewBlock() {
	ts.Close()
	block, _ := ts.tx.Append(ts.filename)
	ts.rp = NewRecordPage(ts.tx, &block, ts.layout)
	ts.currentSlot = -1 // Reset position within new block
}

// Sets an integer value in the current record
func (ts *TableScan) SetInt(fieldname string, val int) {
	ts.rp.SetInt(ts.currentSlot, fieldname, val)
}

// Sets a string value in the current record
func (ts *TableScan) SetString(fieldname string, val string) {
	ts.rp.SetString(ts.currentSlot, fieldname, val)
}

// Creates a new record in the table
func (ts *TableScan) Insert() bool {
	// Attempt to insert in current block after current position
	ts.currentSlot = ts.rp.insertAfter(ts.currentSlot)

	// If no more slots in current block
	for ts.currentSlot < 0 {
		// Check if we're at the last block
		if ts.atLastBlock() {
			ts.moveToNewBlock()
		} else {
			ts.moveToBlock(ts.rp.Block().Number() + 1)
		}
		ts.currentSlot = ts.rp.insertAfter(ts.currentSlot)
	}

	return true
}

// Removes the current record from the table
func (ts *TableScan) Delete() {
	ts.rp.delete(ts.currentSlot)
}

// Checks if the table has a field with the given name
func (ts *TableScan) HasField(fieldname string) bool {
	return ts.layout.Schema().hasField(fieldname)
}

// Positions the scanner at a specific record identified by RID
func (ts *TableScan) MoveToRID(rid any) {
	ts.Close()                                                       // Release current block if any
	block := file.NewBlockID(ts.filename, ts.GetRID().BlockNumber()) // Loads the specified block into memory
	ts.rp = NewRecordPage(ts.tx, block, ts.layout)
	// Positions at the exact slot within the block
	ts.currentSlot = ts.GetRID().slot
}

func (ts *TableScan) GetRID() *RID {
	return NewRID(ts.rp.Block().Number(), ts.currentSlot)
}

// Checks if the current block is the last block of the table
func (ts *TableScan) atLastBlock() bool {
	size, _ := ts.tx.Size(ts.filename)
	return ts.rp.Block().Number() == size-1
}
