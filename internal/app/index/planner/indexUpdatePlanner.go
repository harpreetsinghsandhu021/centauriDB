package planner

import (
	"centauri/internal/app/interfaces"
	"centauri/internal/app/metadata"
	"centauri/internal/app/parse"
	"centauri/internal/app/plan"
	"centauri/internal/app/tx"
)

// Modification of the basic update update planner that dispatches each update statement to the corresponding index planner.
type IndexUpdatePlanner struct {
	plan.UpdatePlanner
	mdm *metadata.MetaDataManager
}

func NewIndexUpdatePlanner(mdm *metadata.MetaDataManager) *IndexUpdatePlanner {
	return &IndexUpdatePlanner{
		mdm: mdm,
	}
}

// Performs an INSERT operation by:
// 1. Creating a new record in the base table
// 2. Updating all relevant indexes for the new record
func (iup *IndexUpdatePlanner) ExecuteInsert(data *parse.InsertData, tx *tx.Transaction) int {
	// Get the target table name from the insert operation
	tableName := data.TableName()

	// Create a plan for accessing the target table
	p := plan.NewTablePlan(tx, tableName, iup.mdm)

	// Open the table scan in update mode and insert a new blank record
	s := p.Open().(interfaces.UpdateScan)
	s.Insert()           // Create space for new record
	rid, _ := s.GetRID() // Get the Record ID of the new record

	// Retrieve all indexes defined on this table
	indexes := iup.mdm.GetIndexInfo(tableName, tx)

	// Get fields and values to insert
	fields := data.Fields()
	values := data.Values()

	if len(fields) != len(values) {
		panic("field/value count mismatch in insert operation")
	}

	// Process each field in the insert operation
	for i, fieldName := range fields {
		// Get the next value from the iterator
		val := values[i]

		// Set the value in the actual record
		s.SetVal(fieldName, val)

		// Update index if exists for this child
		if ii, exists := indexes[fieldName]; exists {
			idx := ii.Open()
			idx.Insert(val, rid)
			idx.Close()
		}
	}

	s.Close()

	return 1
}

// Performs a DELETE operation by:
// 1. Finding all matching records using the provided predicate
// 2. Removing each record's entries from all indexes
// 3. Deleting the actual records
func (iup *IndexUpdatePlanner) ExecuteDelete(data *parse.DeleteData, tx *tx.Transaction) int {
	tableName := data.TableName()

	p := plan.NewTablePlan(tx, tableName, iup.mdm)
	p = plan.NewSelectPlan(p, data.Pred())

	// Retrieve all indexes defined on the table
	indexes := iup.mdm.GetIndexInfo(tableName, tx)

	s := p.Open().(interfaces.UpdateScan)
	count := 0

	// Process each matching record
	for s.Next() {
		// Get the record's identifier
		rid, _ := s.GetRID()

		// Remove this record from all indexes
		for fldName, ii := range indexes {
			// Get the field value from the record
			val := s.GetVal(fldName)

			// Open the index and delete the entry
			idx := ii.Open()
			idx.Delete(val, rid)
			idx.Close()
		}

		// Delete the actual record
		s.Delete()
		count++
	}

	s.Close()

	return count
}

// Performs an UPDATE operation by:
//  1. Finding all matching records using the provided predicate
//  2. For each record:
//     a. Updating the target field value
//     b. Updating the corresponding index (if exists)
//     by removing old entry and adding new entry
func (iup *IndexUpdatePlanner) ExecuteModify(data *parse.ModifyData, tx *tx.Transaction) int {
	tableName := data.TableName()
	fieldName := data.TargetField()

	// Create a plan for the base table and apply the selection predicate
	p := plan.NewTablePlan(tx, tableName, iup.mdm)
	p = plan.NewSelectPlan(p, data.Pred())

	// Check if there's an index on the field being modified
	indexes := iup.mdm.GetIndexInfo(tableName, tx)
	ii := indexes[fieldName]
	idx := ii.Open()

	// Open the scan in update mode
	s := p.Open().(interfaces.UpdateScan)
	count := 0

	// Process each matching record
	for s.Next() {
		// Evaluate the new value expression in the context of current record
		newVal := data.NewValue().Evaluate(s)

		// Get the old value before modification
		oldVal := s.GetVal(fieldName)

		// Update the actual record
		s.SetVal(data.TargetField(), newVal)

		// If there's an index on this field, update it
		if idx != nil {
			rid, _ := s.GetRID()
			// Remove the old index entry and add new one
			idx.Delete(oldVal, rid)
			idx.Insert(newVal, rid)
		}
		count++
	}

	if idx != nil {
		idx.Close()
	}

	s.Close()

	return count
}

// Creates a new table in the database.
// This operation:
// 1. Creates a new table with the specified schema
// 2. Updates the metadata catalog
// Returns:
//   - 0 on successful creation
func (iup *IndexUpdatePlanner) ExecuteCreateTable(data *parse.CreateTableData, tx *tx.Transaction) int {
	iup.mdm.CreateTable(data.TableName(), data.NewSchema(), tx)
	return 0
}

// Creates a new view in the database.
// This operation:
// 1. Creates a new view with the specified definition
// 2. Updates the metadata catalog
// Returns:
//   - 0 on successful creation
func (iup *IndexUpdatePlanner) ExecuteCreateView(data *parse.CreateViewData, tx *tx.Transaction) int {
	iup.mdm.CreateView(data.ViewName(), data.ViewDef(), tx)
	return 0
}

// Creates a new index on a table field
func (iup *IndexUpdatePlanner) ExecuteCreateIndex(data *parse.CreateIndexData, tx *tx.Transaction) int {
	iup.mdm.CreateIndex(data.IndexName(), data.TableName(), data.FieldName(), tx)
	return 0
}
