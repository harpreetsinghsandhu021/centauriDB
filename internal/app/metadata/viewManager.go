package metadata

import (
	"centauri/internal/app/record"
	"centauri/internal/app/record/schema"
	"centauri/internal/app/tx"
)

const MAX_VIEWDEF = 100

// Handles the creation, deletion and management of database views.
// It maintains view definitions in a special system called viewcat.
type ViewManager struct {
	tm *TableManager
}

// Creates a new view manager instance
func NewViewManager(isNew bool, tableMgr *TableManager, tx *tx.Transaction) *ViewManager {
	vm := &ViewManager{
		tm: tableMgr,
	}

	if isNew {
		schema := schema.NewSchema()
		schema.AddStringField("viewname", MAX_NAME)
		schema.AddStringField("viewdef", MAX_VIEWDEF)
		tableMgr.CreateTable("viewcat", schema, tx)
	}
	return vm
}

func (vm *ViewManager) CreateView(viewName string, viewdef string, tx *tx.Transaction) {
	// Get layout of viewcat table
	layout := vm.tm.GetLayout("viewcat", tx)

	// Start scanning viewcat table
	ts := record.NewTableScan(tx, "viewcat", layout)
	defer ts.Close() // Ensure table scan is closed after operation

	// Insert the view definition
	ts.Insert()
	ts.SetString("viewname", viewName)
	ts.SetString("viewdef", viewdef)
}

// Retrieves the definitionof a specific view
func (vm *ViewManager) GetViewDef(viewName string, tx *tx.Transaction) string {
	// Get layout of viewcat table
	layout := vm.tm.GetLayout("viewcat", tx)

	// Start scanning viewcat table
	ts := record.NewTableScan(tx, "viewcat", layout)
	defer ts.Close()

	// Search for the view
	for ts.Next() {
		if ts.GetString("viewname") == viewName {
			return ts.GetString("viewdef")
		}
	}

	return ""
}
