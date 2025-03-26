package file

import "fmt"

// Represents a unique identifier for a block in a file.
// It combines a filename and block number to provide a way to reference
// specific blocks
// This type is immutable once created
type BlockID struct {
	filename    string
	blockNumber int
}

// Creates a new BLOCKID with the specified filename and block number
func NewBlockID(filename string, blockNumber int) *BlockID {
	return &BlockID{
		filename:    filename,
		blockNumber: blockNumber,
	}
}

func (b *BlockID) FileName() string {
	return b.filename
}

func (b *BlockID) Number() int {
	return b.blockNumber
}

// Checks if the blockID is equal to another blockID
func (b *BlockID) Equals(other *BlockID) bool {
	if other == nil {
		return false
	}

	return b.filename == other.filename && b.blockNumber == other.blockNumber
}

// Returns a string representation of the blockID
func (b *BlockID) String() string {
	return fmt.Sprintf("[file %s, block %d]", b.filename, b.blockNumber)
}

// Returns a hash code for the BlockID
// This is used when BlockID is used as a key in maps
func (b *BlockID) HashCode() int {
	h := 0
	for _, c := range b.filename {
		h = 31*h + int(c)
	}

	return h*31 + b.blockNumber
}
