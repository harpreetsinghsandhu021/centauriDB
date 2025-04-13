package multibuffer

import (
	"centauri/internal/app/file"
	"centauri/internal/app/interfaces"
	"centauri/internal/app/record"
	"centauri/internal/app/record/schema"
	"centauri/internal/app/tx"
	"centauri/internal/app/types"
)

// Implements the Scan interface for the "chunk" operator.
// It allows scanning through a range of blocks in a file as a single unit.
type ChunkScan struct {
	interfaces.Scan
	buffs       []*record.RecordPage
	tx          *tx.Transaction
	fileName    string
	layout      record.Layout
	startbnum   int // The starting block number
	endbnum     int // The ending block number
	currentbnum int
	rp          record.RecordPage
	currentSlot int
}

func NewChunkScan(tx *tx.Transaction, filename string, layout record.Layout, startbnum, endbnum int) *ChunkScan {
	cs := &ChunkScan{
		tx:        tx,
		fileName:  filename,
		layout:    layout,
		startbnum: startbnum,
		endbnum:   endbnum,
		buffs:     make([]*record.RecordPage, 0, endbnum-startbnum+1),
	}

	// Initialize all record pages in the range
	for i := startbnum; i <= endbnum; i++ {
		block := file.NewBlockID(filename, i)
		cs.buffs = append(cs.buffs, record.NewRecordPage(tx, block, &layout))
	}

	// Move to the first block
	cs.moveToBlock(startbnum)
	return cs
}

func (cs *ChunkScan) Close() {
	for i := 0; i < len(cs.buffs); i++ {
		block := file.NewBlockID(cs.fileName, cs.startbnum+i)
		cs.tx.Unpin(block)
	}
}

func (cs *ChunkScan) BeforeFirst() {
	cs.moveToBlock(cs.startbnum)
}

// Implements the query.Scan Next method.
// Moves to the next record in the current block of the chunk.
func (cs *ChunkScan) Next() bool {
	cs.currentSlot = cs.rp.NextAfter(cs.currentSlot)

	// If no more slots in current block, move to next block
	for cs.currentSlot < 0 {
		if cs.currentbnum == cs.endbnum {
			return false
		}
		cs.moveToBlock(cs.rp.Block().Number() + 1)
		cs.currentSlot = cs.rp.NextAfter(cs.currentSlot)
	}

	return true
}

// GetInt implements the query.Scan GetInt method.
// Returns the integer value at the specified field for the current record.
func (cs *ChunkScan) GetInt(fldname string) int {
	return cs.rp.GetInt(cs.currentSlot, fldname)
}

// GetString implements the query.Scan GetString method.
// Returns the string value at the specified field for the current record.
func (cs *ChunkScan) GetString(fldname string) string {
	return cs.rp.GetString(cs.currentSlot, fldname)
}

// GetVal implements the query.Scan GetVal method.
// Returns the value at the specified field as a Constant object.
func (cs *ChunkScan) GetVal(fldname string) *types.Constant {
	if cs.layout.Schema().DataType(fldname) == schema.INTEGER {
		return types.NewConstantInt(cs.GetInt(fldname))
	}
	return types.NewConstantString(cs.GetString(fldname))
}

// HasField implements the query.Scan HasField method.
// Returns true if the specified field is in the schema.
func (cs *ChunkScan) HasField(fldname string) bool {
	return cs.layout.Schema().HasField(fldname)
}

// Updates the current block number, record page reference, and resets the slot position.
func (cs *ChunkScan) moveToBlock(blockNum int) {
	cs.currentbnum = blockNum
	cs.rp = *cs.buffs[cs.currentbnum-cs.startbnum]
	cs.currentSlot = -1
}
