package index

import (
	"centauri/internal/app/types"
)

// Defines Operations for database index management
type Index interface {
	// Positions the index cursor before the first entry
	// that matches or exceeds the specified search key
	// searchKey: The key value to position the cursor before
	BeforeFirst(searchKey *types.Constant)

	// Advances the index cursor to the next entry
	// Returns true if there is a next entry, false if at the end
	Next() bool

	// Returns the RID associated with the current index entry
	GetDataRid() *types.RID

	// Adds a new entry to the index
	// dataVal: the key value to insert
	// dataRid: the record ID associated with the key value
	Insert(dataVal *types.Constant, dataRid *types.RID)

	// Removes an entry from the index
	// dataVal: the key value to delete
	// dataRid: the record ID associated with the key value
	Delete(dataVal *types.Constant, dataRid *types.RID)

	// Releases any resources associated with the index
	Close()
}
