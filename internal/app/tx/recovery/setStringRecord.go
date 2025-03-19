package recovery

import (
	"centauri/internal/app/file"
	"centauri/internal/app/log"
	"centauri/internal/app/tx"
	"fmt"
)

// Represents a log record that stores information about a string modification
// in a transaction.
type SetStringRecord struct {
	txnum  int           // Transaction identifier
	offset int           // Position within the block
	val    string        // The string value being set
	block  *file.BlockID // Reference to the modified block
}

// Creates a new log record from a page of bytes
// The page layout is expected to be:
// | RecordType(4) | TxNum(4) | Filename(var) | BlockNum(4) | Offset(4) | Value(var) |
func NewSetStringRecord(p *file.Page) *SetStringRecord {
	// Start at position 4 because first 4 bytes contain record type
	tpos := 4
	// Read the transaction number from position 4
	txnum := p.GetInt(tpos)

	// Calculate position of filename by skipping transaction number(4 bytes)
	fpos := tpos + 4
	// Read the filename string from this position
	filename := p.GetString(fpos)

	// Calculate position of block number by skipping the filename
	// Maxlength returns the space needed for the filename in the page
	bpos := fpos + file.MaxLength(len(filename))
	// Read block number from calculated position
	blockNumber := p.GetInt(bpos)
	// Create new BLOCKID using filename and block number
	block := file.NewBlockID(filename, int(blockNumber))

	// Calculate offset position by skippiong block number(4 bytes)
	offsetPos := bpos + 4
	// Read the offset value
	offset := p.GetInt(offsetPos)

	// Calculate value position by skipping offset(4 bytes)
	vpos := offsetPos + 4
	// Read the actual string value
	val := p.GetString(vpos)

	return &SetStringRecord{
		txnum:  int(txnum),
		offset: int(offset),
		val:    val,
		block:  block,
	}
}

func (r *SetStringRecord) Op() LogRecordType {
	return SETSTRING
}

func (r *SetStringRecord) txNumber() int {
	return r.txnum
}

// Returns a string representation of the record
func (r *SetStringRecord) String() string {
	return fmt.Sprintf("<SETSTRING %d %v %d %s", r.txnum, r.block, r.offset, r.val)
}

func (r *SetStringRecord) Undo(tx tx.Transaction) {
	tx.Pin(r.block)
	tx.SetString(r.block, r.offset, r.val, false) // dont`t log the undo
	tx.Unpin(r.block)
}

// Writes a string modification record to the log.
// The function creates a byte record with the following layout:
// | RecordType(4) | TxNum(4) | Filename(var) | BlockNum(4) | Offset(4) | Value(var) |
func WriteToLog(lm *log.LogManager, txnum int, block *file.BlockID, offset int, val string) (int, error) {
	// Calculate positions for each fields in the record
	tpos := 4        // Skip first 4 bytes (record type)
	fpos := tpos + 4 // Position after txnum
	bpos := fpos +   // Position after filename
		file.MaxLength(len(block.FileName()))
	opos := bpos + 4 // Position after block number
	vpos := opos + 4 // Position after offset

	// Calculate total record length including variable-length strings
	recordLen := vpos + file.MaxLength(len(val))

	// Create a new byte slice of calculate length
	record := make([]byte, recordLen)

	// Create a new page from the byte slice
	p := file.NewPageFromBytes(record)

	// Write all fields to the page in sequence:
	p.SetInt(0, SETSTRING)                // Write record type
	p.SetInt(tpos, int32(txnum))          // Write transaction number
	p.SetString(fpos, block.FileName())   // Write filename
	p.SetInt(bpos, int32(block.Number())) // Write block number
	p.SetInt(opos, int32(offset))         // Write offset
	p.SetString(vpos, val)                // Write string value

	return lm.Append(record)
}
