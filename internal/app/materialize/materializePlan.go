package materialize

import (
	"centauri/internal/app/interfaces"
	"centauri/internal/app/record"
	"centauri/internal/app/record/schema"
	"centauri/internal/app/tx"
	"math"
)

// Implements a query plan that materializes the results of its source plan
// into a temp table. This is useful for operations that need to scan the same
// data multiple times.
// Key characterstics:
// - Creates a physical copy of the source data into the temp table
// - Useful for expensive subqueries that are referenced multiple times
// - Implements the Plan interface for integration with query execution
type MaterializePlan struct {
	interfaces.Plan
	srcPlan interfaces.Plan
	tx      *tx.Transaction
}

func NewMaterializePlan(tx *tx.Transaction, srcPlan interfaces.Plan) interfaces.Plan {
	return &MaterializePlan{
		srcPlan: srcPlan,
		tx:      tx,
	}
}

// Executes the materialization by:
// 1. Creating a temp table with the same schema as the source
// 2. Copying all records from the source to the temp table
// 3. Returning a scan positioned at the beginning of the materialized result
func (mp *MaterializePlan) Open() interfaces.Scan {
	sch := mp.srcPlan.Schema()

	// Create temp table to hold materialized results
	temp := NewTempTable(mp.tx, sch)
	src := mp.srcPlan.Open()
	dest := temp.Open()

	for src.Next() {
		dest.Insert() // Create new record in temp table
		// Copy all field values
		for _, fieldName := range sch.Fields() {
			dest.SetVal(fieldName, src.GetVal(fieldName))
		}
	}

	src.Close()
	// Reset position to beginning before returning
	dest.BeforeFirst()

	return dest
}

// Estimates the number of block accesses required to materialize and read the
// results. The calculation considers:
// - The number of records from the source plan
// - The block size of the transaction
// - The slot size determined by the schema layout
func (mp *MaterializePlan) BlocksAccessed() int {
	// Create layout to determine slot size
	layout := record.NewLayout(mp.srcPlan.Schema())
	// Calculate records per block
	rpb := float64(mp.tx.BlockSize()) / float64(layout.SlotSize())

	return int(math.Ceil(float64(mp.srcPlan.RecordsOutput()) / rpb))
}

// Estimates the number of records in the materialzed result, which is exactly
// the same as the source plan's output count.
func (mp *MaterializePlan) RecordsOutput() int {
	return mp.srcPlan.RecordsOutput()
}

// Estimates the number of distinct values in the materialzed result, which is exactly
// the same as the source plan's distinct values.
func (mp *MaterializePlan) DistinctValues(fieldName string) int {
	return mp.srcPlan.DistinctValues(fieldName)
}

func (mp *MaterializePlan) Schema() *schema.Schema {
	return mp.srcPlan.Schema()
}
