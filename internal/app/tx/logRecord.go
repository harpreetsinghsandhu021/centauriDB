package tx

import (
	"centauri/internal/app/file"
)

// Represents the type of log record
type LogRecordType int

const (
	CHECKPOINT LogRecordType = 0
	START                    = 1
	COMMIT                   = 2
	ROLLBACK                 = 3
	SETINT                   = 4
	SETSTRING                = 5
)

type LogRecord interface {
	Op() LogRecordType
	TxNumber() int
	Undo(txnum *Transaction)
}

// Creates a new log record from bytes
func CreateLogRecord(bytes []byte) LogRecord {
	p := file.NewPageFromBytes(bytes)
	recordType := LogRecordType(p.GetInt(0))

	switch recordType {
	case CHECKPOINT:
		return NewCheckpointRecord()
	case START:
		return NewStartRecord(p)
	case COMMIT:
		return NewCommitRecord(p)
	case ROLLBACK:
		return NewRollbackRecord(p)
	case SETINT:
		return SetIntRecord(p)
	case SETSTRING:
		return NewSetStringRecord(p)
	default:
		return nil
	}
}
