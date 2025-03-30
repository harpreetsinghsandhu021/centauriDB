package metadata

import (
	"centauri/internal/app/index/hash"
	"centauri/internal/app/record"
	sch "centauri/internal/app/record/schema"
	"centauri/internal/app/tx"
)

// The information about an index.
// This information is used by the query planner in order to estimate the costs
// of using the index, and to obtain the layout of the index records/
type IndexInfo struct {
	idxName     string
	fldName     string
	tx          *tx.Transaction
	tableSchema *sch.Schema
	idxLayout   *record.Layout
	si          *StatInfo
}

func NewIndexInfo(idxName string, fldName string, tableSchema *sch.Schema, tx *tx.Transaction, si *StatInfo) *IndexInfo {

	ii := &IndexInfo{
		idxName:     idxName,
		fldName:     fldName,
		tx:          tx,
		tableSchema: tableSchema,
		si:          si,
	}

	ii.idxLayout = ii.createIdxLayout()

	return ii
}

// Open creates and returns a new HashIndex instance for this index.
// It initializes the index using the transaction, index name and layout
// stored in the IndexInfo struct.
func (ii *IndexInfo) Open() interface{} {
	return hash.NewHashIndex(ii.tx, ii.idxName, ii.idxLayout)
}

// Estimate the number of block accesses required to
// find all index records having a particular search key.
// The method is crucial for query optimization as it helps
// the query planner estimate the cost of using this index.
//
// The calculation takes into account:
//   - The block size of the database
//   - The size of each index record (slot size)
//   - The number of records that match the search criteria
//   - The distribution of records across blocks
//
// Returns
//   - int: Estimated number of block accesses needed
func (ii *IndexInfo) BlocksAccessed() int {
	// Calculate Records per Block (rpb)
	// - BlockSize(): gets the size of a disk block in bytes
	// - SlotSize(): gets the size of an index record in bytes
	// - rpb represents how many index records can fit in one block
	rpb := ii.tx.BlockSize() / ii.idxLayout.SlotSize()

	// Calculate the number of blocks needed to store matching records
	// - RecordsOutput(): gets the estimated number of matching records
	// - Division by rpb gives us the number of blocks these records occupy
	numBlocks := ii.si.RecordsOutput() / rpb

	return hash.SearchCost(numBlocks, rpb)
}

// Estimates the number of records that will be retrieved by a
// selection on the indexed field.
// It calculates this by:
// - Getting the total number of records that satisfy the selection
// - Dividing by the number of distinct values in the indexed field
// This gives us the average number of records per distinct value
func (ii *IndexInfo) RecordsOutput() int {
	return ii.si.RecordsOutput() / ii.si.DistinctValues(ii.fldName)
}

// Returns the number of distinct values for a specified field in the index.
// Returns:
//   - 1 if the field is the indexed field (assuming unique index)
//   - Number of distinct values for other fields
func (ii *IndexInfo) DistinctValues(fname string) int {
	if ii.fldName == fname {
		return 1
	}
	return ii.si.DistinctValues(fname)
}

// Creates the physical layout for the index records.
func (ii *IndexInfo) createIdxLayout() *record.Layout {
	// Create new schema for index records
	schema := sch.NewSchema()
	// Add fields for record location
	schema.AddIntField("block") // Block number of the record
	schema.AddIntField("id")    // Record ID within the block

	// Add field for indexed value based on its type
	if ii.tableSchema.DataType(ii.fldName) == sch.INTEGER {
		schema.AddIntField("dataval") // For integer values
	} else {
		// For string values, use the same length as original field
		fldLen := ii.tableSchema.Length(ii.fldName)
		schema.AddStringField("dataval", fldLen)
	}

	return record.NewLayout(schema)
}
