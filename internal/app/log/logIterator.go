package log

import (
	"centauri/internal/app/file"
	"fmt"
)

// LogIterator provides iteration over log records in reverse order
type LogIterator struct {
	fm           *file.FileManager
	currentBlock *file.BlockID
	page         *file.Page
	currentPos   int
	boundary     int
}

// NewLogIterator creates a new iterator for log records
func NewLogIterator(fm *file.FileManager, blk *file.BlockID) *LogIterator {
	iter := &LogIterator{
		fm:           fm,
		currentBlock: file.NewBlockID(blk.FileName(), blk.Number()),
		page:         file.NewPage(fm.BlockSize()),
	}

	// Read the block into the page
	err := fm.Read(iter.currentBlock, iter.page)
	if err != nil {
		// Handle error in production code
		return nil
	}

	// Set the boundary and current position
	iter.boundary = int(iter.page.GetInt(0))
	iter.currentPos = iter.boundary

	return iter
}

// Checks if there are more entries to read in the log.
// It returns true if either:
// - The current position hasn't reached the end of the current block
// - There are previous blocks available (block number > 0)
// Returns false when we've reached the beginning of the log and consumed all entries.
func (li *LogIterator) HasNext() bool {
	return li.currentPos < li.fm.BlockSize() || li.currentBlock.Number() > 0
}

// Returns the next record in the log and advances the iterator position.
// It automatically moves to the previous block if the current position reaches the block size.
// Returns the record as a byte slice and any error encountered.
// If successful, the iterator's position is updated to point to the next record.
func (li *LogIterator) Next() ([]byte, error) {
	// If we've reached the end of the current block
	if li.currentPos == li.fm.BlockSize() {
		// Create a new BlockID for the previous block (moving backwards)
		block := file.NewBlockID(li.currentBlock.FileName(), li.currentBlock.Number()-1)
		// Move the iterator to the previous block and load its data
		li.moveToBlock(block)
	}

	// Get the record bytes at the current position in the page
	rec := li.page.GetBytes(li.currentPos)
	// Advance the position by 4 (integer size) plus the length of the record
	// The 4 bytes represent the record length prefix
	li.currentPos += 4 + len(rec)
	// Return the record bytes and nil error
	return rec, nil
}

// Moves the iterator to the specified block and initializes the reading position.
// It reads the block contents into the page buffer and sets up boundary and current position
// for reading records from the block.
func (li *LogIterator) moveToBlock(block *file.BlockID) error {
	// Read the contents of the specified block into the page buffer
	if err := li.fm.Read(block, li.page); err != nil {
		// If reading fails, return an error with block details and wrapped original error
		return fmt.Errorf("error reading block %v: %w", block, err)
	}

	// Get the boundary value from the first integer (4 bytes) in the page
	// This boundary marks the position where the last record ends
	li.boundary = int(li.page.GetInt(0))

	// Set the current position to the boundary
	// This ensures we start reading from where the last record ends
	li.currentPos = li.boundary

	// Return nil to indicate successful block movement
	return nil
}
