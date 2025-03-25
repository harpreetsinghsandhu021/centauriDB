package tx

import (
	"centauri/internal/app/file"
	"centauri/internal/app/log"
	"fmt"
)

type SetIntRecord struct {
	LogRecord
	txNum  int
	offset int
	val    int
	block  *file.BlockID
}

// Creates a new SetIntRecord by parsing a Page contanining log record data
func NewSetIntRecord(p *file.Page) *SetIntRecord {
	// Position for transaction number(starts after operation type which takes 4 bytes)
	tPos := 4
	txNum := p.GetInt(tPos)

	// Position for fileName (starts after transaction number)
	fPos := tPos + 4
	fileName := p.GetString(fPos)

	// Position for block number (starts after filename)
	// Maxlength calculates the space needed for the string including length prefix
	bPos := fPos + file.MaxLength(len(fileName))
	blockNum := p.GetInt(bPos)
	block := file.NewBlockID(fileName, int(blockNum))

	// Position for offset within block (starts after block number)
	oPos := bPos + 4
	offset := p.GetInt(oPos)

	// Position for the actual value (Starts after offset)
	vPos := oPos + 4
	val := p.GetInt(vPos)

	return &SetIntRecord{
		txNum:  int(txNum),
		offset: int(offset),
		val:    int(val),
		block:  block,
	}
}

func (sir *SetIntRecord) Op() LogRecordType {
	return SETINT
}

func (sir *SetIntRecord) TxNumber() int {
	return sir.txNum
}

func (sir *SetIntRecord) String() string {
	return fmt.Sprintf("<SETINT %t %b %o %v>", sir.txNum, sir.block, sir.offset, sir.val)
}

// Restores the previous value at the specified block and offset.
// It performs the following steps:
// 1. Pins the block to ensure it stays in memory
// 2. Sets the original value back without logging(to prevent infinite undo loops)
// 3. Unpins the block to allow buffer manager to reuse it if needed
func (sir *SetIntRecord) undo(tx *Transaction) {
	// Pin the block to keep it in memory during the operation
	tx.Pin(sir.block)
	// Restore the original value
	// The false parameter prevents this operation being logged to
	// avoid creating an infinite chain of undo records
	tx.SetInt(*sir.block, sir.offset, sir.val, false)

	// Release the block
	tx.Unpin(sir.block)
}

// Writes a SEtInt record to the log.
// This log record contains the SETINT operator,
// followed by the transaction id, the filename, number,
// and offset of the modified block, and the previous integer value at that offset.
func WriteToLogIntRecord(lm *log.LogManager, txNum int, block *file.BlockID, offset int, val int) int {
	tPos := 4
	fPos := tPos + 4
	bPos := fPos + file.MaxLength(len(block.FileName()))
	oPos := bPos + 4
	vPos := oPos + 4

	rec := make([]byte, vPos+4)
	p := file.NewPageFromBytes(rec)

	p.SetInt(0, SETINT)
	p.SetInt(tPos, int32(txNum))
	p.SetString(fPos, block.FileName())
	p.SetInt(bPos, int32(block.Number()))
	p.SetInt(oPos, int32(offset))
	p.SetInt(vPos, int32(val))

	lsn, _ := lm.Append(rec)
	return lsn
}
