package buffer

import (
	"centauri/internal/app/file"
	"centauri/internal/app/log"
)

// Represents an individual buffer. A Buffer wraps a page and stores
// information about its status, such as the associated disk block
// the number of times the buffer has been pinned, whether its contents
// have been modified, and if so, the id and lsn of the mofifying transaction.
type Buffer struct {
	fm       *file.FileManager
	lm       *log.LogManager
	contents *file.Page
	block    *file.BlockID // nil indicates no block assigned
	pins     int
	txnum    int // -1 indicates not modified
	lsn      int // -1 indicates no corresponding log record
}

// Creates a new buffer managed by the specified file and log managers.
func NewBuffer(fm *file.FileManager, lm *log.LogManager) *Buffer {
	return &Buffer{
		fm:       fm,
		lm:       lm,
		contents: file.NewPage(fm.BlockSize()),
		block:    nil,
		pins:     0,
		txnum:    -1,
		lsn:      -1,
	}
}

func (b *Buffer) Contents() *file.Page {
	return b.contents
}

func (b *Buffer) Block() *file.BlockID {
	return b.block
}

// Marks the buffer as having been modified by the specified transaction.
func (b *Buffer) SetModified(txnum int, lsn int) {
	b.txnum = txnum
	if lsn >= 0 {
		b.lsn = lsn
	}
}

// Returns true if the buffer is currently pinned
// i.e if it has a non-zero pin count.
func (b *Buffer) IsPinned() bool {
	return b.pins > 0
}

// Returns the id of the transaction that last modified this buffer.
func (b *Buffer) ModifyingTx() int {
	return b.txnum
}

// Reads the contents of the specified block into
// to the contents of the buffer. If the buffer was dirty, then its previous
// contents are first written to disk.
func (b *Buffer) AssignToBlock(block *file.BlockID) {
	b.Flush()
	b.block = block
	b.fm.Read(block, b.contents)
	b.pins = 0
}

// Writes the buffer to its disk block if it is dirty
func (b *Buffer) Flush() {
	if b.txnum >= 0 {
		b.lm.Flush(b.lsn)
		b.fm.Write(b.block, b.contents)
		b.txnum = -1
	}
}

// Increases the buffer`s pin count
func (b *Buffer) Pin() {
	b.pins++
}

// Decreases the buffer`s pin count
func (b *Buffer) Unpin() {
	b.pins--
}
