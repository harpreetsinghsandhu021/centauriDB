package materialize

import (
	"centauri/internal/app/interfaces"
	"centauri/internal/app/types"
)

// Defines the interface for aggregate operations in the database.
// It provides methods to process rows and retrieve aggregated results.
type AggregateFunction interface {
	// Initializes the aggregation with the first row of data.
	ProcessFirst(s interfaces.Scan)
	// Updates the aggregation state with subsequent rows.
	ProcessNext(s interfaces.Scan)
	// Returns the name of the field being aggregated.
	FieldName() string
	// Returns the current aggregated value as a Constant type.
	value() *types.Constant
}
