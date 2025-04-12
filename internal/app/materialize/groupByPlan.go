package materialize

import (
	"centauri/internal/app/interfaces"
	"centauri/internal/app/record/schema"
	"centauri/internal/app/tx"
)

// Represents a plan for the groupBy operator
// It groups records based on specific fields and applies aggregation functions
type GroupByPlan struct {
	p           interfaces.Plan
	groupFields []string
	aggFns      []AggregateFunction
	sch         *schema.Schema
}

func NewGroupPlan(tx *tx.Transaction, p interfaces.Plan, groupFields []string, aggFns []AggregateFunction) *GroupByPlan {
	// Create a sort plan to ensure records are group properly
	sortedPlan := newSortPlan(tx, p, groupFields)
	// Init schema for the output
	sch := schema.NewSchema()

	for _, fieldName := range groupFields {
		sch.Add(fieldName, p.Schema())
	}
	for _, fn := range aggFns {
		sch.AddIntField(fn.FieldName())
	}

	return &GroupByPlan{
		p:           sortedPlan,
		groupFields: groupFields,
		aggFns:      aggFns,
		sch:         sch,
	}
}

// Opens the plan and returns a Scan object to iterate over the result.
// It returns a GroupByScan that processes the underlying sorted records
func (g *GroupByPlan) Open() interfaces.Scan {
	s := g.p.Open()
	return NewGroupByScan(s, g.groupFields, g.aggFns)
}

func (g *GroupByPlan) BlocksAccessed() int {
	return g.p.BlocksAccessed()
}

// Returns the number of groups. Assuming equal distribution, this is the product
// of the distinct values for each grouping field.
func (g *GroupByPlan) RecordsOutput() int {
	numGroups := 1
	for _, fieldName := range g.groupFields {
		numGroups *= g.p.DistinctValues(fieldName)
	}

	return numGroups
}

// Returns the number of distinct values for the specified field.
// If the field is a grouping field, the the number of distinct values is the same as in
// the underlying query. If the field is an aggregate field, then we assume that all values are distinct.
func (g *GroupByPlan) DistinctValues(fieldName string) int {
	if g.p.Schema().HasField(fieldName) {
		return g.p.DistinctValues(fieldName)
	}

	return g.RecordsOutput()
}

func (g *GroupByPlan) Schema() *schema.Schema {
	return g.sch
}
