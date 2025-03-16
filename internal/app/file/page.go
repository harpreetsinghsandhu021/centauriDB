package file

import (
	"encoding/binary"
	"unicode/utf8"
)

// Represents a page in the databasse that manages data using a byte slice and US_ASCII as character encoding
type Page struct {
	contents []byte
}

// Creates a new page with the specified block size
func NewPage(blockSize int) *Page {
	return &Page{
		contents: make([]byte, blockSize),
	}
}

// Creates a new page from an existing byte slice
func newPageFromBytes(b []byte) *Page {
	return &Page{
		contents: b,
	}
}

// Retrieves an integer from the specified offset
func (p *Page) GetInt(offset int) int32 {
	return int32(binary.BigEndian.Uint32(p.contents[offset : offset+4]))
}

// Reads a byte array from specified offset
// The first 4 bytes at the offset represent the length of the array
func (p *Page) GetBytes(offset int) []byte {
	// Read the length of the byte array
	length := int(binary.BigEndian.Uint32(p.contents[offset : offset+4]))

	// Create a new byte array of the specified length
	b := make([]byte, length)

	// Copy the bytes from the contents
	copy(b, p.contents[offset+4:offset+4+length])

	return b
}

// Reads a string from the specified offset
func (p *Page) GetString(offset int) string {
	// Get the byte array
	b := p.GetBytes(offset)

	// Convert byte array to string
	return string(b)
}

// Writes an integer at the specified offset
func (p *Page) SetInt(offset int, n int32) {
	binary.BigEndian.PutUint32(p.contents[offset:offset+4], uint32(n))
}

// Writes a byte array at specified offset
// The first 4 bytes at the offset will contain the length of the array
func (p *Page) SetBytes(offset int, b []byte) {
	// Write the length of the byte array
	binary.BigEndian.PutUint32(p.contents[offset:offset+4], uint32(len(b)))

	// Write the actual bytes
	copy(p.contents[offset+4:offset+4+len(b)], b)
}

// Writes a string at the specifiied offset
func (p *Page) SetString(offset int, s string) {
	// Convert the string to bytes
	b := []byte(s)

	// Write the bytes
	p.SetBytes(offset, b)
}

// Calculates the maximum length needed to store a string
func (p *Page) MaxLength(strlen int) int {
	// In Go, we can use utf8.UTFMax for the maximum bytes per character
	// However, since we`re using ASCII, we can use 1 byte per character
	bytesPerChar := 1

	// Return the total bytes needed (4 for length + bytes for chars)
	return 4 + (strlen * bytesPerChar)
}

// Calculates the maximum length needed to store a UTF-8 string
func (p *Page) MaxLengthUTF8(strlen int) int {
	// Return the total bytes needed (4 for length + max bytes for UTF-8 chars)
	return 4 + (strlen * utf8.UTFMax)
}

func (p *Page) Contents() []byte {
	return p.contents
}
