package interfaces

import (
	"centauri/internal/app/types"
	"go/constant"
)

// Extends the scan interface to provide modification operations
// on records. It includes all methods from Scan plus additional methods
// for updating, inserting and deleting records.
//
// This interface is used by:
//   - Table scans that modify data
//   - Update operations in queries
//   - Index-bases modifications
//   - Record insertion and deletion operations
type UpdateScan interface {
	Scan

	// Modifies the specified field in the current record
	// using a type-independent constant value.
	SetVal(fieldName string, val constant.Value) error

	// Modifies the specified integer field in the current record.
	SetInt(fieldName string, val int) error

	// Modifies the specified string field in the current record.
	SetString(fieldName string, val string) error

	// Creates a new record in the scan
	// The new record's location is implementation-dependent
	Insert() error

	// Removes the current record from the scan
	Delete() error

	// Returns the Record ID of the current record
	GetRID() (*types.RID, error)

	// Positions the scan to the record with the specified RID.
	MoveToRID(rid *types.RID) error
}
