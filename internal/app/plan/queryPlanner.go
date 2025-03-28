package plan

import (
	"centauri/internal/app/interfaces"
	"centauri/internal/app/parse"
	"centauri/internal/app/tx"
)

// Defines an interface for creating execution plans from parsed query data.
// It is responsible for taking the parsed query information and generating an optimal
// execution plan considering the current transaction context.
type QueryPlanner interface {
	// Generates a Plan object from the parsed query data and transaction context
	CreatePlan(data parse.QueryData, tx *tx.Transaction) interfaces.Plan
}
