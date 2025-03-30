package hash

import (
	"centauri/internal/app/index"
	"centauri/internal/app/record"
	"centauri/internal/app/tx"
	"centauri/internal/app/types"
)

const NUM_BUCKETS = 100 // Number of hash buckets used in the hash index

// Implements a hash-based index structure that maps search key values to record IDs.
// It divides records into buckets based on their hash values for efficient searching/
type HashIndex struct {
	index.Index
	tx        *tx.Transaction
	idxName   string
	layout    *record.Layout
	searchKey *types.Constant
	ts        *record.TableScan
}

func NewHashIndex(tx *tx.Transaction, idxName string, layout *record.Layout) index.Index {
	return &HashIndex{
		tx:      tx,
		idxName: idxName,
		layout:  layout,
	}
}

// Positions the index before the first record having the specified search key.
// It determines the appropriate bucket based on the search key's hash value.
func (hi *HashIndex) BeforeFirst(searchKey *types.Constant) {
	hi.close()
	hi.searchKey = searchKey
	bucket := searchKey.HashCode() % NUM_BUCKETS
	tableName := hi.idxName + string(bucket)
	hi.ts = record.NewTableScan(hi.tx, tableName, hi.layout)
}

// Moves to the next index record having the current search key.
// returns true if there is such a record, false otherwise.
func (hi *HashIndex) Next() bool {
	for hi.ts.Next() {
		if hi.ts.GetVal("dataval") == hi.searchKey {
			return true
		}
	}

	return false
}

// Returns the record ID value stored in the current index record.
// The record ID consists of a block number and slot ID.
func (hi *HashIndex) GetDataRid() *types.RID {
	blockNum := hi.ts.GetInt("block")
	id := hi.ts.GetInt("id")
	return types.NewRID(blockNum, id)
}

// Inserts a new index record having the specified search key and
// record ID values. The record is inserted into the appropriate hash
// bucket based on the search key.
func (hi *HashIndex) Insert(val *types.Constant, rid *types.RID) {
	// Position to correct bucket based on search key
	hi.BeforeFirst(val)
	// Insert new record in the bucket
	hi.ts.Insert()

	// Set the record's fields
	hi.ts.SetInt("block", rid.BlockNumber())
	hi.ts.SetInt("id", rid.Slot())
	hi.ts.SetVal("dataval", val)
}

// Removes the index record having the specified search key and record ID
// values. It searches the appropriate bucket for the matching record and removes it.
func (hi *HashIndex) Delete(val *types.Constant, rid *types.RID) {
	// Position to correct bucket based on search key
	hi.BeforeFirst(val)

	// Search for matching record
	for hi.Next() {
		// If found matching RID, delete the record
		if hi.GetDataRid() == rid {
			hi.ts.Delete()
			return
		}
	}
}

// Closes the current table scan if one exists.
// This is typically called before starting a new scan operation.
func (hi *HashIndex) close() {
	if hi.ts != nil {
		hi.ts.Close()
	}
}

// Estimates the cost of searching an index file having the specified
// - number of blocks and records per block.
// - returns the estimated number of block accesses required for the search.
func SearchCost(numBlocks int, rpb int) int {
	return numBlocks / NUM_BUCKETS
}
