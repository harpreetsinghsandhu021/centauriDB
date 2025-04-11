package plan

import (
	"centauri/internal/app/interfaces"
	"centauri/internal/app/metadata"
	"centauri/internal/app/record"
	"centauri/internal/app/record/schema"
	"centauri/internal/app/tx"
)

// TablePlan represents a basic table access plan in a query execution.
// It implements the Plan interface and provides access to table-level operations.
type TablePlan struct {
	interfaces.Plan
	tx        *tx.Transaction
	tableName string
	layout    *record.Layout
	si        *metadata.StatInfo
}

func NewTablePlan(tx *tx.Transaction, tableName string, md *metadata.MetaDataManager) interfaces.Plan {

	layout := md.GetLayout(tableName, tx)
	si := md.GetStatInfo(tableName, layout, tx)

	return &TablePlan{
		tx:        tx,
		tableName: tableName,
		layout:    layout,
		si:        &si,
	}
}

func (tp *TablePlan) Open() interfaces.Scan {
	return record.NewTableScan(tp.tx, tp.tableName, tp.layout)
}

func (tp *TablePlan) BlocksAccessed() int {
	return tp.si.BlocksAccessed()
}

func (tp *TablePlan) RecordsOutput() int {
	return tp.si.RecordsOutput()
}

func (tp *TablePlan) DistinctValues(fieldName string) int {
	return tp.si.DistinctValues(fieldName)
}

func (tp *TablePlan) Schema() *schema.Schema {
	return tp.layout.Schema()
}
