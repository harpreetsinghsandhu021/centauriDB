package metadata

import (
	"centauri/internal/app/record"
	"centauri/internal/app/tx"
)

// Handles the creation  and management of indexes in the database.
// It maintains a catalog of all indexes (idxcat) and provides methods to:
// - Create new indexes
// - Retrieve existing indexes
// - Map between tables and their indexes
// - Manage index statistics
type IndexManager struct {
	layout *record.Layout
	tm     *TableManager
	sm     *StatManager
}

// Creates a new index manager instance.
// For new databases, it creates the index catalog table.
// For existing databases, it loads the existing catalog.
func NewIndexManager(isNew bool, tm *TableManager, sm *StatManager, tx *tx.Transaction) *IndexManager {
	if isNew {
		schema := record.NewSchema()
		schema.AddStringField("indexname", MAX_NAME)
		schema.AddStringField("tablename", MAX_NAME)
		schema.AddStringField("fieldname", MAX_NAME)
		tm.CreateTable("idxcat", schema, tx)
	}

	return &IndexManager{
		tm:     tm,
		sm:     sm,
		layout: tm.GetLayout("idxcat", tx),
	}
}

// Creates a new index entry in the index catalog.
// This method adds a record to the idxcat table with information about:
// - The name of the index
// - The table being indexed
// - The field being indexed
func (im *IndexManager) CreateIndex(idxName string, tableName string, fieldName string, tx *tx.Transaction) {
	ts := record.NewTableScan(tx, "idxcat", im.layout)
	ts.Insert()
	ts.SetString("indexname", idxName)
	ts.SetString("tablename", tableName)
	ts.SetString("fieldname", fieldName)
	ts.Close()
}

// Retrieves information about all indexes on a specified table.
// It scans the index catalog and creates IndexInfo objects for each index found.
func (im *IndexManager) GetIndexInfo(tableName string, tx *tx.Transaction) map[string]IndexInfo {
	result := make(map[string]IndexInfo)
	ts := record.NewTableScan(tx, "idxcat", im.layout)

	// Scan through all index catalog records
	for ts.Next() {
		// Check if this index belongs to the specified table
		if ts.GetString("tablename") == tableName {
			// Get index details
			idxName := ts.GetString("indexname")
			fldName := ts.GetString("fieldname")

			// Get table information
			tableLayout := im.tm.GetLayout(tableName, tx)
			tableStat := im.sm.GetStatInfo(tableName, tableLayout, tx)

			// Create index information object
			indexInfo := *NewIndexInfo(idxName, fldName, tableLayout.Schema(), tx, &tableStat)

			// Store in result map, keyed by field name
			result[fldName] = indexInfo
		}
	}
	ts.Close()
	return result
}
