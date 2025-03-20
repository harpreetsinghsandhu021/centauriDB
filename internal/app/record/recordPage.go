package record

import (
	"centauri/internal/app/file"
	"centauri/internal/app/tx"
)

const EMPTY = 0 // Indicates unused/deleted record slot
const USED = 1  // Indicates an active record slot

// Represents a page of records in the database
// It manages the physical storage and retrieval of records within a block
type RecordPage struct {
	tx     *tx.Transaction
	block  *file.BlockID
	layout *Layout
}

// Creates and initializes a new Recordpage instance
func NewRecordPage(tx *tx.Transaction, block *file.BlockID, layout *Layout) *RecordPage {
	rp := &RecordPage{
		tx:     tx,
		block:  block,
		layout: layout,
	}

	tx.Pin(*block)

	return rp
}

func (rp *RecordPage) Block() *file.BlockID {
	return rp.block
}

// Returns the integer value stored for the specified field of a specified slot.
func (rp *RecordPage) GetInt(slot int, fieldname string) int {
	// Calculate the exact byte position for the field
	fieldPos := rp.offset(slot) + rp.layout.Offset(fieldname)
	value, _ := rp.tx.GetInt(*rp.block, fieldPos)
	return int(value)
}

// Returns the string value stored for the specified field of the specified slot.
func (rp *RecordPage) GetString(slot int, fieldname string) string {
	fieldPos := rp.offset(slot) + rp.layout.Offset(fieldname)
	value, _ := rp.tx.GetString(*rp.block, fieldPos)
	return value
}

// Stores an integer value in the specified field of a record slot
func (rp *RecordPage) SetInt(slot int, fieldname string, val int) {
	fieldPos := rp.offset(slot) + rp.layout.Offset(fieldname)
	rp.tx.SetInt(*rp.block, fieldPos, val, true)
}

// Stores a string value in the specified field of a record slot
func (rp *RecordPage) SetString(slot int, fieldname string, val string) {
	fieldPos := rp.offset(slot) + rp.layout.Offset(fieldname)
	rp.tx.SetString(*rp.block, fieldPos, val, true)
}

// Initializes the block, making all slots empty and setting default values
// for all record fields. This is called when the block is first allocated.
func (rp *RecordPage) format() {
	slot := 0
	for rp.isValidSlot(slot) {
		// Set the slot flag to EMPTY
		rp.tx.SetInt(*rp.block, rp.offset(slot), int(EMPTY), false)

		// Initialize all fields in the slot
		schema := rp.layout.Schema()
		for _, fieldname := range schema.Fields() {
			fieldPos := rp.offset(slot) + rp.layout.Offset(fieldname)
			if schema.dataType(fieldname) == INTEGER {
				rp.tx.SetInt(*rp.block, fieldPos, 0, false)
			} else {
				rp.tx.SetString(*rp.block, fieldPos, "", false)
			}
		}
		slot++
	}
}

// Marks a slot as empty (deleted)
func (rp *RecordPage) delete(slot int) {
	rp.setFlag(slot, EMPTY)
}

// Returns the next used slot after the specified slot
func (rp *RecordPage) nextAfter(slot int) int {
	return rp.searchAfter(slot, USED)
}

// Finds the next empty slot after the specified slot and marks
// it as used
func (rp *RecordPage) insertAfter(slot int) int {
	newSlot := rp.searchAfter(slot, EMPTY)
	if newSlot >= 0 {
		rp.setFlag(newSlot, USED)
	}
	return newSlot
}

func (rp *RecordPage) offset(slot int) int {
	return slot * rp.layout.slotSize
}

// Checks if a slot number is within the block`s capacity
func (rp *RecordPage) isValidSlot(slot int) bool {
	return rp.offset(slot+1) <= rp.tx.BlockSize()
}

// Sets the status flag (EMPTY/USED) for a slot
func (rp *RecordPage) setFlag(slot int, flag int) {
	rp.tx.SetInt(*rp.block, rp.offset(slot), int(flag), true)
}

// Finds the next slot within the specified flag value
func (rp *RecordPage) searchAfter(slot int, flag int) int {
	slot++ // Start searching from the next slot
	for rp.isValidSlot(slot) {
		slotFlag, _ := rp.tx.GetInt(*rp.block, rp.offset(slot))

		if int(slotFlag) == flag {
			return slot
		}
		slot++
	}

	return -1
}
