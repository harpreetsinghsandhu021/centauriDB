package interfaces

import (
	"centauri/internal/app/record/schema"
)

// Plan represents a query execution plan in the database system.
// It provides methods to analyze and execute different aspects of query processing.
// Including cost estimation and actual data process operations.
type Plan interface {
	// Intializes and returns a new Scan object that provides access to the
	// records produced by this plan. Each call to Open creates a new scan
	// that can be used to iterate over the result set.
	Open() Scan

	// Returns the estimated number of disk blocks that need to be read when executing
	// this plan. This is crucial for cost-based query optimization decisions.
	BlocksAccessed() int

	// Returns the number of records that this plan will produce. This helps in determining
	//  the size of result sets and making optimization decisions for subsequent operations.
	RecordsOutput() int

	// Returns the estimated number of distinct values that will appear in the specified field
	//  within the records produced by this plan. This is valuable for selectivity estimation
	// and join optimization.
	DistinctValues(fieldName string) int

	// Returns the Schema object describing the structure of the records that this plan produces.
	// This includes information about field names, types, and constraints.
	Schema() *schema.Schema
}
