package planner

import (
	"centauri/internal/app/index/query"
	"centauri/internal/app/interfaces"
	"centauri/internal/app/metadata"
	"centauri/internal/app/record"
	"centauri/internal/app/record/schema"
	"centauri/internal/app/types"
)

// Represents a plan node for index selection operations.
// It corresponds to the indexselect relational algebra operator.
type IndexSelectPlan struct {
	interfaces.Plan
	p   interfaces.Plan
	ii  *metadata.IndexInfo
	val types.Constant
}

func NewIndexSelectPlan(p interfaces.Plan, ii *metadata.IndexInfo, val types.Constant) *IndexSelectPlan {
	return &IndexSelectPlan{
		p:   p,
		ii:  ii,
		val: val,
	}
}

// Creates a new indexselect scan for this query.
// It panics if the underlying plan is not a TableScan.
func (isp *IndexSelectPlan) Open() *query.IndexSelectScan {
	ts, ok := isp.p.Open().(*record.TableScan)
	if !ok {
		panic("IndexSelectPlan requires a TableScan as input")
	}

	idx := isp.ii.Open()

	return query.NewIndexSelectScan(ts, idx, isp.val)
}

// The number of block accesses to compute the index selection, which
// is the same as the index traversal cost plus the number of matching
// data records.
func (isp *IndexSelectPlan) BlocksAccessed() int {
	return isp.ii.BlocksAccessed() + isp.RecordsOutput()
}

// Estimates the number of output records in the index selection,
// which is the same as the number of search key values for the index.
func (isp *IndexSelectPlan) RecordsOutput() int {
	return isp.ii.RecordsOutput()
}

func (isp *IndexSelectPlan) DistinctValues(fldName string) int {
	return isp.ii.DistinctValues(fldName)
}

func (isp *IndexSelectPlan) Schema() *schema.Schema {
	return isp.p.Schema()
}
