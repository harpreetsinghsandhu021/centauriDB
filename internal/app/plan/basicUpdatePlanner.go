package plan

import (
	"centauri/internal/app/interfaces"
	"centauri/internal/app/metadata"
	"centauri/internal/app/parse"
	"centauri/internal/app/tx"
)

// Implements basic database update operations like delete, modify, insert
// and DDL operations like create table, view and index. It uses MetadDataManager
// to handle table metadata operations.
type BasicUpdatePlanner struct {
	mdm *metadata.MetaDataManager
}

func NewBasicUpdatePlanner(mdm *metadata.MetaDataManager) *BasicUpdatePlanner {
	return &BasicUpdatePlanner{
		mdm: mdm,
	}
}

// Performs a delete operation on records that match a given predicate.
// This operation follows these steps:
// 1. Creates a table plan for accessing the target table
// 2. Adds a Selection plan to filter records based on the predicate
// 3. Scans through matching records and deletes them
// 4. Keeps track of the number of deleted records
// Returns:
//   - Number of records deleted
//
// Example:
//
//	data might contain: DELETE FROM students WHERE age > 20
//	This would delete all student records where age is greater than 20
func (bup *BasicUpdatePlanner) ExecuteDelete(data *parse.DeleteData, tx *tx.Transaction) int {
	// Create a table plan for accessing the specified table
	// This provides the basic infrastructure for reading table records
	p := NewTablePlan(tx, data.TableName(), bup.mdm)

	// Add a selection plan that filters records based on the predicate
	// This ensures we only process records that match our WHERE clause
	sp := NewSelectPlan(p, data.Pred())

	// Open an update scan that allows both reading and writing records
	// Type assertion ensures we have update capabilities
	us := sp.Open().(interfaces.UpdateScan)
	count := 0

	// Delete each matching record
	for us.Next() {
		us.Delete()
		count++
	}

	us.Close()
	return count
}

// Performs an update operation on records that match a given predicate.
// It should update specific fields with new values for all matching records.
// This operation follows these steps:
// 1. Creates a table plan for accessing the target table
// 2. Adds a selection plan to filter records based on the predicate
// 3. Updates matching records with new values
// 4. Keeps track of the number of modifief records
// Returns:
//   - Number of records modified
//
// Example:
//
//	ModifyData might contain: UPDATE students SET age = 21 WHERE id = 1
func (bup *BasicUpdatePlanner) ExecuteModify(data *parse.ModifyData, tx *tx.Transaction) int {
	p := NewTablePlan(tx, data.TableName(), bup.mdm)

	sp := NewSelectPlan(p, data.Pred())

	us := sp.Open().(interfaces.UpdateScan)
	count := 0

	for us.Next() {
		val := data.NewValue().Evaluate(us)
		us.SetVal(data.TargetField(), val)
		count++
	}

	us.Close()
	return count
}

// Performs an insert operation into the specified table.
// This operation follows these steps:
// 1. Creates a table plan for the target table
// 2. Creates a new record
// 3. Sets values for all specific fields
// Returns :
//   - 1 If successfull (since only record is inserted at a time)
//
// Example:
//
//	InsertData might contain: INSERT INTO students (id, name, age) VALUES (1, "John", 20)
func (bup *BasicUpdatePlanner) ExecuteInsert(data *parse.InsertData, tx *tx.Transaction) int {
	p := NewTablePlan(tx, data.TableName(), bup.mdm)
	us := p.Open().(interfaces.UpdateScan)

	// Open an update scan
	us.Insert()

	for i, fieldName := range data.Fields() {
		val := data.Values()[i]
		us.SetVal(fieldName, val)
	}

	us.Close()
	return 1
}

// Creates a new table in the database.
// This operation:
// 1. Creates a new table with the specified schema
// 2. Updates the metadata catalog
// Returns:
//   - 0 on successful creation
func (bup *BasicUpdatePlanner) ExecuteCreateTable(data *parse.CreateTableData, tx *tx.Transaction) int {
	bup.mdm.CreateTable(data.TableName(), data.NewSchema(), tx)
	return 0
}

// Creates a new view in the database.
// This operation:
// 1. Creates a new view with the specified definition
// 2. Updates the metadata catalog
// Returns:
//   - 0 on successful creation
func (bup *BasicUpdatePlanner) ExecuteCreateView(data *parse.CreateViewData, tx *tx.Transaction) int {
	bup.mdm.CreateView(data.ViewName(), data.ViewDef(), tx)
	return 0
}

// Creates a new index on a table field
func (bup *BasicUpdatePlanner) ExecuteCreateIndex(data *parse.CreateIndexData, tx *tx.Transaction) int {
	bup.mdm.CreateIndex(data.IndexName(), data.TableName(), data.FieldName(), tx)
	return 0
}
