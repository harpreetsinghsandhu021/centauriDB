package tx

import (
	"centauri/internal/app/file"
	"centauri/internal/app/log"
	"encoding/binary"
	"fmt"
)

type StartRecord struct {
	LogRecord
	txNum int
}

func NewStartRecord(p *file.Page) *StartRecord {
	tPos := 1

	return &StartRecord{
		txNum: int(p.GetInt(tPos)),
	}
}

func Op() int {
	return START
}

func (sr *StartRecord) TxNumber() int {
	return sr.txNum
}

func undo(tx *Transaction)

func (sr *StartRecord) String() string {
	return fmt.Sprintf("<START %t>", sr.txNum)
}

// Writes a start record to the log ansd returns the LSN of the last log value
func (sr *StartRecord) WriteToLog(lm *log.LogManager, txNum int) int {
	// Create a byte slice with capacity for two 32-bit integers
	rec := make([]byte, 8)

	// Convert integers to bytes and write them to the slice
	binary.LittleEndian.PutUint32(rec[0:4], uint32(START))
	binary.LittleEndian.PutUint32(rec[4:8], uint32(txNum))

	// Append to log and return position
	res, _ := lm.Append(rec)
	return res
}
