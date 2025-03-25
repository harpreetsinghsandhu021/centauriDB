package tx

import (
	"centauri/internal/app/buffer"
	"centauri/internal/app/file"
	"centauri/internal/app/log"
	"fmt"
	"sync/atomic"
)

var nextTxNum atomic.Int64 // Global atomic counter for transaction numbers
const EndOfFile = -1       // Represents the end of file marker for block operations

// Represents an individual database transaction. It coordinates buffer management,
// recovery, and concurrency control
type Transaction struct {
	rm        *RecoveryManager
	cm        *ConcurrencyManager
	bm        *buffer.BufferManager
	fm        *file.FileManager
	lm        *log.LogManager
	txnum     int64
	myBuffers *BufferList
}

func NewTransaction(fm *file.FileManager, lm *log.LogManager, bm *buffer.BufferManager) *Transaction {
	txNum := nextTmNumber()

	tx := &Transaction{
		fm:    fm,
		bm:    bm,
		txnum: txNum,
		lm:    lm,
	}

	tx.rm = tx.rm.NewRecoveryManager(tx, txNum, lm, bm)
	tx.cm = NewConcurrencyManager(nil)
	tx.myBuffers = NewBufferList(bm)

	return tx
}

// Commit finalizes the transaction by:
// - Committing all changes through the recovery manager
// - Printing a confirmation message with the transaction number
// - Releasing all locks through the concurrency manager
// - Unpinning all buffers associated with the transaction
func (tx *Transaction) Commit() {
	tx.rm.Commit()
	fmt.Printf("transaction %d committed\n", tx.txnum)
	tx.cm.Release()
	tx.myBuffers.UnpinAll()
}

// Aborts the current transaction, releasing all locks, unpinning buffers,
// and rolling back any changes made during the transaction. After rollback,
// the transaction will be terminated and cannot be used anymore.
// It rolls back any changes through the recovery manager,
// releases all locks held by the transaction through the concurrency manager,
// and unpins any buffers used during the transaction.
func (tx *Transaction) Rollback() {
	tx.rm.Rollback()
	fmt.Printf("transaction %d rolled back\n", tx.txnum)
	tx.cm.Release()
	tx.myBuffers.UnpinAll()
}

// Performs a transaction recovery operation by first flushing all pending changes
// to disk via the buffer manager and then executing recovery procedures through the
// recovery manager. This method is typically called after a system crash or failure
// to restore the transaction to a consistent state.
func (tx *Transaction) Recover() {
	tx.bm.FlushAll(int(tx.txnum))
	tx.rm.Recover()
}

// Pins a block to prevent it from being discarded
// Parameters:
//   - block: The BlockID of the block to be unpinned
func (tx *Transaction) Pin(block file.BlockID) {
	tx.myBuffers.Pin(block)
}

// Unpins indicates that a block is no longer needed
func (tx *Transaction) Unpin(block *file.BlockID) {
	tx.myBuffers.Unpin(*block)
}

// DATA ACCESS OPERATIONS

// Retrieves an integer value from a specific block at the given offset.
func (tx *Transaction) GetInt(block file.BlockID, offset int) (int32, error) {
	// Acquire a SLock since this is a read operations
	// Multiple transactions can read the same block simultaneously
	if err := tx.cm.SLock(block); err != nil {
		return 0, err
	}

	// Get the buffer containing the block data
	// This pins the buffer, preventing it from being replaced
	buff, err := tx.myBuffers.GetBuffer(block)
	if err != nil {
		return 0, err
	}

	// Read and return the integer value at the specified offset
	return buff.Contents().GetInt(offset), nil
}

// Retrieves string values with shared locking
func (tx *Transaction) GetString(block file.BlockID, offset int) (string, error) {
	// Acquire shared locks for concurrent reads
	if err := tx.cm.SLock(block); err != nil {
		return "", err
	}

	// Get and pin buffer
	buff, err := tx.myBuffers.GetBuffer(block)
	if err != nil {
		return "", err
	}

	return buff.Contents().GetString(offset), nil
}

// Writes integer value with exclusive locking
func (tx *Transaction) SetInt(block file.BlockID, offset int, val int, okToLog bool) error {
	// Axcquire exclusive lock for writing,
	// Only one transaction can write to this block at a time
	if err := tx.cm.XLock(block); err != nil {
		return err
	}

	// Get and pin buffer to prevent it from being replalced
	// while we're modifying its contents
	buff, err := tx.myBuffers.GetBuffer(block)
	if err != nil {
		return err
	}

	lsn := -1 // Log sequence number for recovery tracking

	// If logging is enabled, create a recovery log entry
	// This ensures durability in case of crashes
	if okToLog {
		lsn = tx.rm.SetInt(buff, offset, val)
	}

	// Get the page contents and update the interger value
	// at the specified offset
	p := buff.Contents()
	p.SetInt(offset, int32(val))

	// Mark the buffer as modified with this transaction's ID
	// and the log sequence number for recovery purposes
	buff.SetModified(int(tx.txnum), lsn)
	return nil
}

// Writes a string value to a specific block location with exclusive locking
func (tx *Transaction) SetString(block file.BlockID, offset int, val string, okToLog bool) error {
	// Acquire exclusive lock for writing to prevent concurrent modifications
	if err := tx.cm.XLock(block); err != nil {
		return err
	}

	// Get and pin the buffer containing our target block
	buff, err := tx.myBuffers.GetBuffer(block)
	if err != nil {
		return err
	}

	// Track modifications for recovery if logging is enabled
	lsn := -1
	if okToLog {
		lsn = tx.rm.SetString(buff, offset, val)
	}

	// Update the string value in the buffer's contents
	p := buff.Contents()
	p.SetString(offset, val)

	// Mark buffer as modified for this transaction
	buff.SetModified(int(tx.txnum), lsn)

	return nil
}

// Returns the number of blocks in a file, using shared locking
func (tx *Transaction) Size(filename string) (int, error) {
	// Create a dummy block for the end of the file
	// We lock this to prevent concurrent file modifications
	dummyBlock := file.NewBlockID(filename, EndOfFile)

	// Acquire shared lock since we're only reading file metadata
	if err := tx.cm.SLock(*dummyBlock); err != nil {
		return 0, err
	}

	// Get the file length in blocks and return if no error
	length, err := tx.fm.Length(filename)
	if err != nil {
		return 0, err
	}

	return length, nil
}

// Appends a new block to the end of a file with exclusive locking
func (tx *Transaction) Append(filename string) (file.BlockID, error) {
	// Create a dummy block for EOF position
	dummyBlock := file.NewBlockID(filename, EndOfFile)

	// Get exclusive lock since we're modifying file structure
	if err := tx.cm.XLock(*dummyBlock); err != nil {
		return file.BlockID{}, err
	}

	// Append new block and returns its ID
	block, err := tx.fm.Append(filename)
	if err != nil {
		return file.BlockID{}, err
	}
	return *block, nil
}

// Returns the system's block size in bytes
func (tx *Transaction) BlockSize() int {
	// This is a constant value that does`nt need locking
	return tx.fm.BlockSize()
}

// Returns the current number of free buffers in the pool
func (tx *Transaction) AvailableBuffers() int {
	// Get current count of available buffers
	// No locking needed as this is informational only
	return tx.bm.Available()
}

// Generates the next transaction number automatically
func nextTmNumber() int64 {
	next := nextTxNum.Add(1)
	fmt.Printf("new transaction: %d\n", next)
	return next
}
