package record

import (
	"centauri/internal/app/file"
	"unsafe"
)

// Represents the physical layout of records according to a schema.
type Layout struct {
	schema   *Schema
	offsets  map[string]int
	slotSize int
}

// Creates a layout object from the schema.
// This function is used when a table is created.
// It determines the physical offset of each field within the record.
func NewLayout(schema *Schema) *Layout {
	offsets := make(map[string]int)

	// Leave Space for the empty/in-use flag
	pos := int(unsafe.Sizeof(int(0)))

	for _, fieldName := range schema.Fields() {
		offsets[fieldName] = pos
		pos += lengthInBytes(schema, fieldName)
	}

	return &Layout{
		schema:   schema,
		offsets:  offsets,
		slotSize: pos,
	}
}

// Creates a layout object from the specified metadata.
// This function is used when the metadata is retrieved from the catalog.
func NewLayoutWithOffsets(schema *Schema, offsets map[string]int, slotSize int) *Layout {
	return &Layout{
		schema:   schema,
		offsets:  offsets,
		slotSize: slotSize,
	}
}

func (l *Layout) Schema() *Schema {
	return l.schema
}

// Returns the byte offset of the specified field
func (l *Layout) Offset(fieldname string) int {
	offset, exists := l.offsets[fieldname]
	if !exists {
		return -1
	}

	return offset
}

// Returns the size of a slot
func (l *Layout) SlotSize() int {
	return l.slotSize
}

// Returns the number of bytes required to store the specified field
func lengthInBytes(schema *Schema, fieldname string) int {
	fieldType := schema.DataType(fieldname)

	if fieldType == INTEGER {
		return int(unsafe.Sizeof(int(0)))
	} else {
		return file.MaxLength(schema.Length(fieldname))
	}
}
