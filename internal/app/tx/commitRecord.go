package tx

import (
	"centauri/internal/app/file"
	"centauri/internal/app/log"
	"encoding/binary"
	"fmt"
)

type CommitRecord struct {
	LogRecord
	txNum int
}

func NewCommitRecord(p *file.Page) *StartRecord {
	tPos := 4

	return &StartRecord{
		txNum: int(p.GetInt(tPos)),
	}
}

// Returns the operation type constant for COMMIT operations
// This helps identify the record type when reading from the log.
func (cr *CommitRecord) Op() int {
	return COMMIT
}

func (cr *CommitRecord) TxNumber() int {
	return cr.txNum
}

// Defines how to reverse a COMMIT operation
// Does nothing because a start record contains no undo information.
func (cr *CommitRecord) undo(tx *Transaction)

func (cr *CommitRecord) String() string {
	return fmt.Sprintf("<START %t>", cr.txNum)
}

// Writes a commit record to the transaction log.
// The record is written as 8 bytes:
//   - First 4 bytes: COMMIT operation code
//   - Last 4 bytes:  Transaction number
//
// Returns:
//   - LSN (Log sequence number) of the written record
func writeToLogCommitRecord(lm *log.LogManager, txNum int) int {
	// Create a byte slice with capacity for two 32-bit integers
	rec := make([]byte, 8)

	// Convert integers to bytes and write them to the slice
	binary.LittleEndian.PutUint32(rec[0:4], uint32(COMMIT))
	binary.LittleEndian.PutUint32(rec[4:8], uint32(txNum))

	// Append to log and return position
	lsn, _ := lm.Append(rec)
	return lsn
}
