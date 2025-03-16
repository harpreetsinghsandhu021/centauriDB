package file

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

type FileManager struct {
	dbDirectory string              // Directory where database files are stored
	blockSize   int                 // Size of each block in bytes
	isNew       bool                // Indicates if database is new
	openFiles   map[string]*os.File // Cache of open files for quick access
	mu          sync.Mutex          // Mutex for thread safety
}

// NewFileManager initializes the file manager
// It creates the directory if new and cleans temporary files
func NewFileManager(dbDirectory string, blockSize int) (*FileManager, error) {
	fm := &FileManager{
		dbDirectory: dbDirectory,
		blockSize:   blockSize,
		openFiles:   make(map[string]*os.File),
	}

	// Check if database is new
	info, err := os.Stat(dbDirectory)
	if os.IsNotExist(err) {
		fm.isNew = true
		// Create directory if it doesn't exist
		if err := os.MkdirAll(dbDirectory, 0755); err != nil {
			return nil, fmt.Errorf("cannot create directory: %w", err)
		}
	} else if err != nil {
		return nil, fmt.Errorf("cannot access directory: %w", err)
	} else if !info.IsDir() {
		return nil, fmt.Errorf("%s is not a directory", dbDirectory)
	}

	// Clean up temporary files if directory exists
	if !fm.isNew {
		entries, err := os.ReadDir(dbDirectory)
		if err != nil {
			return nil, fmt.Errorf("cannot read directory: %w", err)
		}

		for _, entry := range entries {
			if strings.HasPrefix(entry.Name(), "temp") {
				path := filepath.Join(dbDirectory, entry.Name())
				if err := os.Remove(path); err != nil {
					return nil, fmt.Errorf("cannot remove temporary file %s: %w", path, err)
				}
			}
		}
	}

	return fm, nil
}

// Read a block from disk into a page
func (fm *FileManager) Read(blk *BlockID, p *Page) error {
	// Acquire lock for thread safety when accessing shared resources
	fm.mu.Lock()
	// Release lock when method returns
	defer fm.mu.Unlock()

	// Get or create file handle for the given filename
	file, err := fm.getFile(blk.FileName())
	if err != nil {
		return fmt.Errorf("cannot get file: %w", err)
	}

	// Calculate offset in bytes where block starts
	// offset = block number * block size
	offset := int64(blk.Number()) * int64(fm.blockSize)
	if _, err := file.Seek(offset, 0); err != nil {
		return fmt.Errorf("cannot seek to position: %w", err)
	}

	// Read block data into page contents
	n, err := file.Read(p.contents)
	if err != nil {
		return fmt.Errorf("cannot read block %v: %w", blk, err)
	}

	// Verify complete block was read
	// Number of bytes read should match block size
	if n != fm.blockSize {
		return fmt.Errorf("partial read for block %v: got %d bytes, expected %d", blk, n, fm.blockSize)
	}

	return nil
}

// Writes a page to a block on disk
func (fm *FileManager) Write(blk *BlockID, p *Page) error {
	fm.mu.Lock()
	defer fm.mu.Unlock()

	file, err := fm.getFile(blk.FileName())
	if err != nil {
		return fmt.Errorf("cannot get file: %w", err)
	}

	offset := int64(blk.Number()) * int64(fm.blockSize)
	if _, err := file.Seek(offset, 0); err != nil {
		return fmt.Errorf("cannot seek to position: %w", err)
	}

	n, err := file.Write(p.contents)
	if err != nil {
		return fmt.Errorf("cannot write block %v: %w", blk, err)
	}

	if n != fm.blockSize {
		return fmt.Errorf("partial write for block %v: wrote %d bytes, expected %d", blk, n, fm.blockSize)
	}

	// Ensure written data is flushed from OS buffers to disk
	if err := file.Sync(); err != nil {
		return fmt.Errorf("cannot sync file: %w", err)
	}

	return nil
}

// Append appends a new block to a file
func (fm *FileManager) Append(filename string) (*BlockID, error) {
	fm.mu.Lock()
	defer fm.mu.Unlock()

	// Get new block number
	length, err := fm.Length(filename)
	if err != nil {
		return nil, err
	}

	blk := &BlockID{filename: filename, blockNumber: length}

	// Create empty block
	emptyData := make([]byte, fm.blockSize)

	file, err := fm.getFile(filename)
	if err != nil {
		return nil, fmt.Errorf("cannot get file: %w", err)
	}

	offset := int64(blk.Number()) * int64(fm.blockSize)
	if _, err := file.Seek(offset, 0); err != nil {
		return nil, fmt.Errorf("cannot seek to position: %w", err)
	}

	n, err := file.Write(emptyData)
	if err != nil {
		return nil, fmt.Errorf("cannot append block %v: %w", blk, err)
	}

	if n != fm.blockSize {
		return nil, fmt.Errorf("partial write for block %v: wrote %d bytes, expected %d", blk, n, fm.blockSize)
	}

	// Ensure data is flushed to disk
	if err := file.Sync(); err != nil {
		return nil, fmt.Errorf("cannot sync file: %w", err)
	}

	return blk, nil
}

// Length gets number of blocks in a file
func (fm *FileManager) Length(filename string) (int, error) {
	file, err := fm.getFile(filename)
	if err != nil {
		return 0, fmt.Errorf("cannot get file: %w", err)
	}

	info, err := file.Stat()
	if err != nil {
		return 0, fmt.Errorf("cannot stat file %s: %w", filename, err)
	}

	return int(info.Size()) / fm.blockSize, nil
}

// getFile gets or creates a file for a filename
func (fm *FileManager) getFile(filename string) (*os.File, error) {
	// Check cache first
	if file, ok := fm.openFiles[filename]; ok {
		return file, nil
	}

	// Create or open the file if not in cache
	path := filepath.Join(fm.dbDirectory, filename)
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, fmt.Errorf("cannot open file %s: %w", path, err)
	}

	fm.openFiles[filename] = file
	return file, nil
}

// Close closes all open files
func (fm *FileManager) Close() error {
	fm.mu.Lock()
	defer fm.mu.Unlock()

	var lastErr error
	for name, file := range fm.openFiles {
		if err := file.Close(); err != nil {
			lastErr = fmt.Errorf("error closing %s: %w", name, err)
		}
		delete(fm.openFiles, name)
	}
	return lastErr
}

// IsNew returns whether the database directory was newly created
func (fm *FileManager) IsNew() bool {
	return fm.isNew
}

// BlockSize returns the block size in bytes
func (fm *FileManager) BlockSize() int {
	return fm.blockSize
}
