package tx

import (
	"centauri/internal/app/log"
	"encoding/binary"
)

type CheckPointRecord struct {
	LogRecord
}

func NewCheckpointRecord() *CheckPointRecord {

	return &CheckPointRecord{}
}

func (cp *CheckPointRecord) Op() LogRecordType {
	return CHECKPOINT
}

// Checkpoint records have no associated transaction,
// andn so the method returns a dummy, negative txid.
func (cp *CheckPointRecord) TxNumber() int {
	return -1
}

// Defines how to reverse a CHECKPOINT operation
// Does nothing because a checkpoint record contains no undo information.
func (cp *CheckPointRecord) undo(tx *Transaction) {}

func (cp *CheckPointRecord) String() string {
	return "<CHECKPOINT>"
}

// Writes a commit record to the transaction log.
// The record is written as 8 bytes:
//   - First 4 bytes: COMMIT operation code
//   - Last 4 bytes:  Transaction number
//
// Returns:
//   - LSN (Log sequence number) of the written record
func writeToLogCheckpointRecord(lm *log.LogManager, txNum int) int {
	// Create a byte slice with capacity for two 32-bit integers
	rec := make([]byte, 8)

	// Convert integers to bytes and write them to the slice
	binary.LittleEndian.PutUint32(rec[0:4], uint32(CHECKPOINT))
	binary.LittleEndian.PutUint32(rec[4:8], uint32(txNum))

	// Append to log and return position
	lsn, _ := lm.Append(rec)
	return lsn
}
