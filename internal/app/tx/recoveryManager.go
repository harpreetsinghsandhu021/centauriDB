package tx

import (
	"centauri/internal/app/buffer"
	"centauri/internal/app/log"
)

type RecoveryManager struct {
	lm          *log.LogManager
	bm          *buffer.BufferManager
	transaction *Transaction
	txnum       int64
}

func (rm *RecoveryManager) NewRecoveryManager(
	tx *Transaction,
	txnum int64,
	lm *log.LogManager,
	bm *buffer.BufferManager) *RecoveryManager {

	recoveryManager := &RecoveryManager{
		lm:          lm,
		bm:          bm,
		transaction: tx,
		txnum:       txnum,
	}

	StartRecord.writeToLog(lm, txnum)

	return recoveryManager
}

func (rm *RecoveryManager) Commit() {
	rm.bm.FlushAll(rm.txnum)
	lsn := CommitRecord.writeToLog(rm.lm, rm.txnum)
	rm.lm.Flush(lsn)
}

func (rm *RecoveryManager) Rollback() {
	doRollback()
	rm.bm.FlushAll(rm.txnum)
	lsn := RollbackRecord.writeToLog(rm.lm, rm.txnum)
	rm.lm.Flush(lsn)
}

func (rm *RecoveryManager) Recover() {
	doRecover()
	rm.bm.FlushAll(rm.txnum)
	lsn := CheckPointRecord.writeToLog(rm.lm)
	rm.lm.Flush(lsn)
}

func (rm *RecoveryManager) SetInt(buff buffer.Buffer, offset int, newval int) {
	oldval := buff.Contents().GetInt(offset)
	block := buff.Block()

	return SetInRecord.writeToLog(rm.lm, rm.txnum, block, offset, oldval)
}

func (rm *RecoveryManager) SetString(buff buffer.Buffer, offset int, newval string) {
	oldVal := buff.Contents().GetString(offset)
	block := buff.Block()
	return SetStringRecord.writeToLog(rm.lm, rm.txnum, block, offset, oldVal)
}

// Performs a rollback operation for a specific transaction.
// It scans the log backwards until it finds the START record for the transaction,
// undoing all operations for that transaction along the way.
func (rm *RecoveryManager) doRollback() {
	// Get an iterator to scan through log records
	iter, _ := rm.lm.Iterator()

	// Iterate through all log records
	for iter.HasNext() {
		bytes, _ := iter.Next()
		record := CreateLogRecord(bytes)

		// Only process records for this specific transaction
		if record.TxNumber() == rm.txnum {
			// If we find the START record, we`re done
			// as we`ve undone all operations after the start
			if record.Op() == START {
				return
			}
			// Undo this operation
			record.Undo(rm.transaction)
		}
	}
}

// Performs crash recovery using the UNDO-only recovery strategy.
// It scans the log backwards, undoing all uncommitted transactions until
// it reaches a CHECKPOINT record.
func (rm *RecoveryManager) doRecover() {
	// Map to track transactions that have completed (committed or rolled back)
	// Using map[int]struct{} for memory efficiency as we only need to track existence
	finishedTxns := make(map[int]struct{})

	iter, _ := rm.lm.Iterator()

	for iter.HasNext() {
		bytes, _ := iter.Next()
		record := CreateLogRecord(bytes)

		// If we hit a CHECKPOINT, recovery is complete
		// as we dont need to process any records before the checkpoint
		if record.Op() == CHECKPOINT {
			return
		}

		// If record is COMMIT or ROLLBACK, mark transaction as finished
		if record.Op() == COMMIT || record.Op() == ROLLBACK {
			// Add transaction number to finished set using empty struct
			finishedTxns[record.TxNumber()] = struct{}{}
		} else {
			// For all other operations,
			// Check if this transaction was not finished (not in finishedTxs)
			if _, exists := finishedTxns[record.TxNumber()]; !exists {
				// If transaction was`nt finished, undo this operation
				record.Undo(rm.transaction)
			}
		}

	}
}
