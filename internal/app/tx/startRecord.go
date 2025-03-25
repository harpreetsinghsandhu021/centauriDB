package tx

import (
	"centauri/internal/app/file"
	"centauri/internal/app/log"
	"encoding/binary"
	"fmt"
)

// Represents the beginning of a transaction in the log.
// It contains the transaction number and implements the LogRecord interface.
type StartRecord struct {
	LogRecord
	txNum int
}

func NewStartRecord(p *file.Page) *StartRecord {
	tPos := 4

	return &StartRecord{
		txNum: int(p.GetInt(tPos)),
	}
}

// Returns the operation type constant for START operations
// This helps identify the record type when reading from the log.
func (sr *StartRecord) Op() LogRecordType {
	return START
}

func (sr *StartRecord) TxNumber() int {
	return sr.txNum
}

// Defines how to reverse a START operation
// Does nothing because a start record contains no undo information.
func undo(tx *Transaction)

func (sr *StartRecord) String() string {
	return fmt.Sprintf("<START %t>", sr.txNum)
}

// Writes a start record to the transaction log.
// The record is written as 8 bytes:
//   - First 4 bytes: START operation code
//   - Last 4 bytes:  Transaction number
//
// Returns:
//   - LSN (Log sequence number) of the written record
func writeToLogStartRecord(lm *log.LogManager, txNum int) int {
	// Create a byte slice with capacity for two 32-bit integers
	rec := make([]byte, 8)

	// Convert integers to bytes and write them to the slice
	binary.LittleEndian.PutUint32(rec[0:4], uint32(START))
	binary.LittleEndian.PutUint32(rec[4:8], uint32(txNum))

	// Append to log and return position
	lsn, _ := lm.Append(rec)
	return lsn
}
