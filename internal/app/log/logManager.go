package log

import (
	"centauri/internal/app/file"
	"fmt"
	"sync"
)

// LogManager manages the system log
// Handles writing and reading of log records with recovery support
type LogManager struct {
	fm           *file.FileManager // file manager reference
	logfile      string            // name of log file
	logpage      *file.Page        // current log page in memory
	currentBlock *file.BlockID     // current block being written
	latestLSN    int               // Latest log sequence number
	lastSavedLSN int               // Last saved log sequence number
	mu           sync.Mutex        // mutex for thread safety
}

// NewLogManager initializes the log manager
// It creates or opens an existing log file
func NewLogManager(fm *file.FileManager, logfile string) (*LogManager, error) {
	logManager := &LogManager{
		fm:      fm,
		logfile: logfile,
	}

	// Create new page with block size
	logManager.logpage = file.NewPage(fm.BlockSize())

	// Check if log exists
	logSize, err := fm.Length(logfile)
	if err != nil {
		return nil, fmt.Errorf("error checking log size: %w", err)
	}

	if logSize == 0 {
		// Create a new log block if log is empty
		currentBlock, err := logManager.appendNewBlock()
		if err != nil {
			return nil, fmt.Errorf("error appending new block: %w", err)
		}
		logManager.currentBlock = currentBlock
	} else {
		// Read last block of existing log
		logManager.currentBlock = file.NewBlockID(logfile, logSize-1)
		if err := fm.Read(logManager.currentBlock, logManager.logpage); err != nil {
			return nil, fmt.Errorf("error reading last block: %w", err)
		}
	}

	return logManager, nil
}

// Append adds a new log record to the log file
// It returns the assigned log sequence number
func (lm *LogManager) Append(logrec []byte) (int, error) {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	boundary := int(lm.logpage.GetInt(0)) // current write position
	recsize := len(logrec)                // size of new record
	bytesneeded := recsize + 4            // total space needed (record + size)

	// Check if record fits in current block
	if boundary-bytesneeded < 4 {
		// if not, flush and create a new block
		if err := lm.flush(); err != nil {
			return 0, fmt.Errorf("error flushing log: %w", err)
		}

		currentBlock, err := lm.appendNewBlock()
		if err != nil {
			return 0, fmt.Errorf("error appending new block: %w", err)
		}

		lm.currentBlock = currentBlock
		boundary = int(lm.logpage.GetInt(0))
	}

	recpos := boundary - bytesneeded    // calculate write position
	lm.logpage.SetBytes(recpos, logrec) // write record
	lm.logpage.SetInt(0, int32(recpos)) // Update record boundary

	lm.latestLSN++
	return lm.latestLSN, nil
}

// appendNewBlock creates and initializes a new log block
func (lm *LogManager) appendNewBlock() (*file.BlockID, error) {
	// Append new block to log file
	block, err := lm.fm.Append(lm.logfile)
	if err != nil {
		return nil, fmt.Errorf("error appending block: %w", err)
	}

	// Initialize boundary to block size
	lm.logpage.SetInt(0, int32(lm.fm.BlockSize()))

	if err := lm.fm.Write(block, lm.logpage); err != nil {
		return nil, fmt.Errorf("error writing new block: %w", err)
	}

	return block, nil
}

// Flush ensures all logs up to specified LSN are written to disk
func (lm *LogManager) Flush(lsn int) error {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	if lsn >= lm.lastSavedLSN {
		return lm.flush()
	}
	return nil
}

// Iterator returns an iterator over log records
func (lm *LogManager) Iterator() (*LogIterator, error) {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	if err := lm.flush(); err != nil {
		return nil, fmt.Errorf("error flushing log: %w", err)
	}

	return NewLogIterator(lm.fm, lm.currentBlock), nil
}

// flush writes the current log page to disk
func (lm *LogManager) flush() error {
	if err := lm.fm.Write(lm.currentBlock, lm.logpage); err != nil {
		return fmt.Errorf("error writing log page: %w", err)
	}
	lm.lastSavedLSN = lm.latestLSN
	return nil
}
