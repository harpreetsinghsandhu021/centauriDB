package interfaces

import "centauri/internal/app/types"

// Scan defines the interface that will be implemented by each query scan.
// Each relational algebra operator(selection, projection, join, etc.) has its
// own implementation of this interface.
//
// This interface provide methods to:
// - Navigate through records (beforeFirst, next)
// - Access Field values (getInt, getString, getVal)
// - Query metadata (hasField)
// - Manage resources
//
// This is the foundational interface for the query processing engine,
// allowing uniform access to records regardless of their source or
// the operations being performed on them.
type Scan interface {
	// Positions the scan before its first record.
	// After this call, as subsequent Next() will return the first record.
	// This method is typically used to reset a scan for repeated processing.
	BeforeFirst()

	// Advances the scan to the next record
	// Returns:
	//   - true if there is a next record
	//   - false if there are no more records
	Next() bool

	// Returns the value of the specified integer field in the current record.
	GetInt(fieldName string) (int, error)

	// Returns the value of the specified string field in the current record.
	GetString(fieldName string) (string, error)

	// Returns the value of the specified field as a constant.
	// This method provides a type-independent way to access field values.
	GetVal(fieldName string) (types.Constant, error)

	// Checks if the scan contains the specified field
	HasField(fieldName string) bool

	// Releases any resourves held by this scan
	// This includes:
	//  - Closing any subscans
	//  - Releasing any buffeers or temporary storage
	//  - Cleaning up any system resources
	// The scan cannot be used after being closed
	Close()
}
