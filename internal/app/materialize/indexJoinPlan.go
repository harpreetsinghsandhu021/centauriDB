package materialize

import (
	"centauri/internal/app/interfaces"
	"centauri/internal/app/record/schema"
	"centauri/internal/app/tx"
	"math"
)

// Represents the plan for the mergejoin operator.
// It joins two sorted plans based on equality of specified fields.
type MergeJoinPlan struct {
	p1       interfaces.Plan // The LHS query plan
	p2       interfaces.Plan // The RHS query plan
	fldName1 string          // The LHS join field
	fldName2 string          // The RHS join field
	sch      *schema.Schema  // schema for joined result
}

func NewMergeJoinPlan(tx *tx.Transaction, p1 interfaces.Plan, p2 interfaces.Plan, fldname1 string, fldName2 string) *MergeJoinPlan {
	// Create sort list for first plan with single field
	sortList1 := []string{fldname1}
	sortedP1 := newSortPlan(tx, p1, sortList1)

	// Create sort list for second plan with single field
	sortList2 := []string{fldName2}
	sortedP2 := newSortPlan(tx, p2, sortList2)

	// Create the merged schema
	sch := schema.NewSchema()
	sch.AddAll(p1.Schema())
	sch.AddAll(p2.Schema())

	return &MergeJoinPlan{
		p1:       sortedP1,
		p2:       sortedP2,
		fldName1: fldname1,
		fldName2: fldName2,
		sch:      sch,
	}
}

// Sorts its two underlying scans on their join field.
// It then returns a mergejoin scan of the two sorted table scans.
func (m *MergeJoinPlan) Open() interfaces.Scan {
	s1 := m.p1.Open()
	s2 := m.p2.Open().(*SortScan)

	return NewMergeJoinScan(s1, s2, m.fldName1, m.fldName2)
}

func (m *MergeJoinPlan) BlocksAccessed() int {
	return m.p1.BlocksAccessed() + m.p2.BlocksAccessed()
}

func (m *MergeJoinPlan) RecordsOutput() int {
	maxvals := math.Max(
		float64(m.p1.DistinctValues(m.fldName1)),
		float64(m.p2.DistinctValues(m.fldName2)),
	)
	return int(float64(m.p1.RecordsOutput()*m.p2.RecordsOutput()) / maxvals)
}

func (m *MergeJoinPlan) DistinctValues(fldname string) int {
	if m.p1.Schema().HasField(fldname) {
		return m.p1.DistinctValues(fldname)
	} else {
		return m.p2.DistinctValues(fldname)
	}
}

func (m *MergeJoinPlan) Schema() *schema.Schema {
	return m.sch
}
