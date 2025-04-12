package materialize

import (
	"centauri/internal/app/record"
	"centauri/internal/app/record/schema"
	"centauri/internal/app/tx"
	"strconv"
	"sync"
)

// Represents a temporary table in the database system.
// Temp Tables are used for immediate results during query processing.
// Key characterstics:
// - Automatically generates unique table names
// - Manages its own table layout/schema
// - Tied to a specific transaction
// - Provides read/write access via UpdateScan
type TempTable struct {
	tx        *tx.Transaction
	tableName string
	layout    *record.Layout
}

// Tracks the counter for generating unique temp table names.
var nextTableNum int64

// Mutex to protect name generation
var nameMutex sync.Mutex

func NewTempTable(tx *tx.Transaction, sch *schema.Schema) *TempTable {
	return &TempTable{
		tx:        tx,
		tableName: generateTableName(),
		layout:    record.NewLayout(sch),
	}
}

// Creates and return an UpdateScan for accessing the temp table.
// The scan provides both read and write capabilities.
func (tt *TempTable) Open() *record.TableScan {
	return record.NewTableScan(tt.tx, tt.tableName, tt.layout)
}

// Returns the system-generated name of this temp table.
// The name is unique within the database instance.
func (tt *TempTable) TableName() string {
	return tt.tableName
}

// Returns the physical layout information for this temp table.
func (tt *TempTable) GetLayout() *record.Layout {
	return tt.layout
}

// Creates a unique name for each temp table.
func generateTableName() string {
	nameMutex.Lock()
	defer nameMutex.Unlock()

	nextTableNum++
	return "temp" + strconv.FormatInt(nextTableNum, 10)
}
