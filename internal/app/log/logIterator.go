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

// HasNext returns true if there are more log records to read
func (li *LogIterator) HasNext() bool {
	return li.currentPos > 4 || li.currentBlock.Number() > 0
}

// Next returns the next log record
func (li *LogIterator) Next() ([]byte, error) {
	// If we've read all records in the current block, move to the previous block
	if li.currentPos <= 4 {
		if li.currentBlock.Number() == 0 {
			return nil, fmt.Errorf("no more records")
		}

		li.currentBlock = file.NewBlockID(li.currentBlock.FileName(), li.currentBlock.Number()-1)
		if err := li.fm.Read(li.currentBlock, li.page); err != nil {
			return nil, fmt.Errorf("error reading previous block: %w", err)
		}

		li.boundary = int(li.page.GetInt(0))
		li.currentPos = li.boundary
	}

	// Read the record
	li.currentPos -= 4
	recSize := int(li.page.GetInt(li.currentPos))
	li.currentPos -= recSize
	return li.page.GetBytes(li.currentPos), nil
}

func (li *LogIterator) moveToBlock(block *file.BlockID) error {
	if err := li.fm.Read(block, li.page); err != nil {
		return fmt.Errorf("error reading block %v: %w", block, err)
	}
	li.boundary = int(li.page.GetInt(0))
	li.currentPos = li.boundary
	return nil
}
