package planner

import (
	"centauri/internal/app/interfaces"
	"centauri/internal/app/metadata"
	"centauri/internal/app/record"
	"centauri/internal/app/record/schema"
)

// Represents a plan node for index join operations.
// It corresponds to the indexjoin relational algebra operator.
type IndexJoinPlan struct {
	p1        interfaces.Plan
	p2        interfaces.Plan
	ii        *metadata.IndexInfo
	joinField string
	schema    *schema.Schema
}

func NewIndexJoinPlan(p1 interfaces.Plan, p2 interfaces.Plan, ii *metadata.IndexInfo, joinField string) *IndexJoinPlan {
	sch := schema.NewSchema()
	sch.AddAll(p1.Schema())
	sch.AddAll(p2.Schema())

	return &IndexJoinPlan{
		p1:        p1,
		p2:        p2,
		ii:        ii,
		joinField: joinField,
		schema:    sch,
	}
}

// Creates an indexjoin scan for this query.
// It panics if the right-hand plan is not a TableScan.
func (ijp *IndexJoinPlan) Open() interfaces.Scan {
	s := ijp.p1.Open()
	ts, ok := ijp.p2.Open().(*record.TableScan)
	if !ok {
		panic("IndexJoinPlan requires right-hand plan to be a TableScan")
	}

	idx := ijp.ii.Open()
	return NewIndexJoinScan(s, idx, ijp.joinField, ts)
}

// Estimates the number of block accesses to compute the join.
// The formula is: B(indexjoin(p1, p2, idx)) = B(p1) + R(p1)*B(idx) + R(indexjoin(p1,p2,idx))
func (ijp *IndexJoinPlan) BlocksAccessed() int {
	return ijp.p1.BlocksAccessed() + (ijp.p1.RecordsOutput() * ijp.ii.BlocksAccessed()) + ijp.ii.RecordsOutput()
}

// Estimates the number of output records in the join.
// The formula is: R(indexjoin(p1,p2,idx)) = R(p1)*R(idx)
func (ijp *IndexJoinPlan) RecordsOutput() int {
	return ijp.p1.RecordsOutput() * ijp.ii.RecordsOutput()
}

// Estimates the number of distinct values for the specified field.
func (ijp *IndexJoinPlan) DistinctValues(fldName string) int {
	if ijp.p1.Schema().HasField(fldName) {
		return ijp.p1.DistinctValues(fldName)
	}

	return ijp.p2.DistinctValues(fldName)
}

func (ijp *IndexJoinPlan) Schema() *schema.Schema {
	return ijp.schema
}
