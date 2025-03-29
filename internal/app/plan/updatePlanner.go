package plan

import (
	"centauri/internal/app/parse"
	"centauri/internal/app/tx"
)

// Defines the interface for executing various database modification operations.
// It handles all non-query operations like INSERT, DELETE, CREATE TABLE, etc.
// Each method returns the number of rows affected by the operation.
type UpdatePlanner interface {
	// Processes am INSERT operation and adds new records to the table
	ExecuteInsert(data *parse.InsertData, tx *tx.Transaction) int

	// Removes records from a table based on specific conditions
	ExecuteDelete(data *parse.DeleteData, tx *tx.Transaction) int

	// Updates existing records in a table
	ExecuteModify(data *parse.ModifyData, tx *tx.Transaction) int

	// Creates a new table in the database
	ExecuteCreateTable(data *parse.CreateTableData, tx *tx.Transaction) int

	// Creates a new view in the database
	ExecuteCreateView(data *parse.CreateViewData, tx *tx.Transaction) int

	// Creates a new index on specified table columns
	ExecuteCreateIndex(data *parse.CreateIndexData, tx *tx.Transaction) int
}
