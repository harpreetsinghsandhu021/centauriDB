package plan

import (
	"centauri/internal/app/interfaces"
	"centauri/internal/app/query"
	"centauri/internal/app/record/schema"
)

// Implements a filtering operation in the query execution plan
// It wraps another Plan and applies a predicate to filter records.
type SelectPlan struct {
	interfaces.Plan
	p    interfaces.Plan  // The underlying plan to be filtered
	pred *query.Predicate // The predicate used for filtering records
}

func NewSelectPlan(p interfaces.Plan, pred *query.Predicate) *SelectPlan {
	return &SelectPlan{
		p:    p,
		pred: pred,
	}
}

// Intiializes the scan operation for this plan.
// It creates a new SelectScan that wraps the underlying plan's scan
// and applies the filtering predicate
func (sp *SelectPlan) Open() interfaces.Scan {
	s := sp.p.Open()
	return query.NewSelectScan(s, sp.pred)
}

// Returns the number of disk blocks that need to be read
// for a select operation, this is the same as the underlying plan
// since we need to scan all blocks to apply the predicate.
func (sp *SelectPlan) BlocksAccessed() int {
	return sp.p.BlocksAccessed()
}

// Estimates the number of records that will be output by this select
// operation. It divides the number of records from the underlying plan
// by the reduction factor of the predicate.
func (sp *SelectPlan) RecordsOutput() int {
	return sp.p.RecordsOutput() / sp.pred.ReductionFactor(sp.p)
}

// Estimates the number of distinct values for a given field in the output
// of this select operation. This estimate depends on:
// 1. If the field is compared to a constant (returns 1)
// 2. If the field is compared to another field (returns min of both fields)
// 3. Otherwise returns the distinct values from the underlying plan
func (sp *SelectPlan) DistinctValues(fieldName string) int {
	if sp.pred.EquatesWithConstant(fieldName) != nil {
		return 1
	} else {
		fieldName2 := sp.pred.EquatesWithField(fieldName)
		if fieldName2 != "" {
			return min(sp.p.DistinctValues(fieldName), sp.p.DistinctValues(fieldName2))
		} else {
			return sp.p.DistinctValues(fieldName)
		}
	}
}

// Returns the schema of the output records
func (sp *SelectPlan) Schema() *schema.Schema {
	return sp.p.Schema()
}
