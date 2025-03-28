package plan

import (
	"centauri/internal/app/interfaces"
	"centauri/internal/app/metadata"
	"centauri/internal/app/parse"
	"centauri/internal/app/tx"
)

// Implements the QueryPlanner interface and provides functionality to create
// execution plans for SQL queries
type BasicQueryPlanner struct {
	QueryPlanner
	mdm *metadata.MetaDataManager
}

func NewBasicQueryPlanner(mdm *metadata.MetaDataManager) *BasicQueryPlanner {
	return &BasicQueryPlanner{
		mdm: mdm,
	}
}

// Generates an execution plan for the given query
// Parameters:
//   - data: contains the parsed query information
//   - tx:   transaction context in which the plan will be executed
//
// Returns:   Plan interdace representing the execution strategy
func (bqp *BasicQueryPlanner) CreatePlan(data *parse.QueryData, tx *tx.Transaction) interfaces.Plan {
	// Create plans array to hold individual table/view plans
	plans := []interfaces.Plan{}

	// Create a plan for each mentioned table or view
	for _, tableName := range data.Tables() {
		// Check if the table name refers to a view
		viewDef := bqp.mdm.GetViewDef(tableName, tx)

		if viewDef != "" {
			// Handle view - recursively plan the view definition
			parser := parse.NewParser(viewDef)
			viewData := parser.Query()
			plans = append(plans, bqp.CreatePlan(viewData, tx))
		} else {
			// Handle base table - create a table plan
			plans = append(plans, NewTablePlan(tx, tableName, bqp.mdm))
		}
	}

	// Create the product of all table plans
	// Start with the first plan
	if len(plans) == 0 {
		return nil // Or handle empty plans case appropriately
	}

	p := plans[0]
	// Combine with remaining plans using product
	for i := 1; i < len(plans); i++ {
		p = NewProductPlan(p, plans[i])
	}

	// Add a selection plan for the predicate
	p = NewSelectPlan(p, data.Pred())

	// Project on the field name
	return NewProjectPlan(p, data.Fields())
}
