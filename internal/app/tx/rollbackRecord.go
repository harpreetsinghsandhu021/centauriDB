package tx

import (
	"centauri/internal/app/file"
	"centauri/internal/app/log"
	"encoding/binary"
	"fmt"
)

// Represents the beginning of a transaction in the log.
// It contains the transaction number and implements the LogRecord interface.
type RollbackRecord struct {
	LogRecord
	txNum int
}

func NewRollbackRecord(p *file.Page) *RollbackRecord {
	tPos := 4
	return &RollbackRecord{
		txNum: int(p.GetInt(tPos)),
	}
}

// Returns the operation type constant for ROLLBACK operations
// This helps identify the record type when reading from the log.
func (rb *RollbackRecord) Op() LogRecordType {
	return ROLLBACK
}

func (rb *RollbackRecord) TxNumber() int {
	return rb.txNum
}

// Defines how to reverse a ROLLBACK operation
// Does nothing because a rollback record contains no undo information.
func (rb *RollbackRecord) undo(tx *Transaction) {}

func (rb *RollbackRecord) String() string {
	return fmt.Sprintf("<START %t>", rb.txNum)
}

// Writes a rollback record to the transaction log.
// The record is written as 8 bytes:
//   - First 4 bytes: START operation code
//   - Last 4 bytes:  Transaction number
//
// Returns:
//   - LSN (Log sequence number) of the written record
func writeToLogRollbackRecord(lm *log.LogManager, txNum int) int {
	// Create a byte slice with capacity for two 32-bit integers
	rec := make([]byte, 8)

	// Convert integers to bytes and write them to the slice
	binary.LittleEndian.PutUint32(rec[0:4], uint32(ROLLBACK))
	binary.LittleEndian.PutUint32(rec[4:8], uint32(txNum))

	// Append to log and return position
	lsn, _ := lm.Append(rec)
	return lsn
}
