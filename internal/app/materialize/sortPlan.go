package materialize

import (
	"centauri/internal/app/interfaces"
	"centauri/internal/app/record"
	"centauri/internal/app/record/schema"
	"centauri/internal/app/tx"
)

// Implements a query plan that sorts the results of an underlying query.
// It uses an external merge-sort algorithmn that:
// 1. Splits the input into sorted runs
// 2. Merges runs in iterations until only 1 or 2 runs remain
// 3. Returns a SortScan that can merge the final runs on demand
type SortPlan struct {
	interfaces.Plan
	tx   *tx.Transaction
	p    interfaces.Plan
	sch  *schema.Schema
	comp *RecordComparator
}

func newSortPlan(tx *tx.Transaction, p interfaces.Plan, sortFields []string) *SortPlan {
	return &SortPlan{
		tx:   tx,
		p:    p,
		sch:  p.Schema(),
		comp: NewRecordComparator(sortFields),
	}
}

// Executes the sort operation using an external merge-sort algorithmn:
// 1. Splits input into sorted runs (each in a temp table)
// 2. Repeatedly merges until 1-2 remain
// 3. Returns a SortScan that merges the final runs
func (sp *SortPlan) Open() interfaces.Scan {
	// Open source Scan and split into initial sorted runs
	src := sp.p.Open()
	runs := sp.SplitIntoRuns(src)
	src.Close()

	// Merge runs in iterations until we have 1-2 runs left
	for len(runs) > 2 {
		runs = sp.doMergeIteration(runs)
	}

	// Returns a scan that will merge the final runs
	return NewSortScan(runs, sp.comp)
}

// Estimates the number of blocks needed to store the sorted results.
// Note: This does'nt include the temporary blocks using during sorting.
func (sp *SortPlan) BlocksAccessed() int {
	// Same as materializing the results (does'nt count sorting overhead)
	mp := NewMaterializePlan(sp.tx, sp.p)
	return mp.BlocksAccessed()
}

// Estimates the number of records in the sorted output.
func (sp *SortPlan) RecordsOutput() int {
	return sp.p.RecordsOutput()
}

// Estimates distinct values for a field in the sorted output.
func (sp *SortPlan) DistinctValues(fldname string) int {
	return sp.p.DistinctValues(fldname)
}

func (sp *SortPlan) Schema() *schema.Schema {
	return sp.sch
}

// Divides the input scan into sorted runs stored in temp tables.
// Each run contains records in sorted order, with new runs starting when the
// sort order would be violated.
func (sp *SortPlan) SplitIntoRuns(src interfaces.Scan) []*TempTable {
	var runs []*TempTable
	src.BeforeFirst()

	// Return empty if no records
	if !src.Next() {
		return runs
	}

	// Create first run
	currentTemp := NewTempTable(sp.tx, sp.sch)
	runs = append(runs, currentTemp)
	currentScan := currentTemp.Open()

	// Process all records
	for {
		// Copy current record to current run
		sp.copyRecord(src, currentScan)

		// Check if next record belongs in this run
		if !src.Next() {
			break
		}

		if sp.comp.Compare(src, currentScan) < 0 {
			// Start new run
			currentScan.Close()
			currentTemp = NewTempTable(sp.tx, sp.sch)
			runs = append(runs, currentTemp)
			currentScan = currentTemp.Open()
		}
	}

	currentScan.Close()
	return runs
}

// Performs one merge iteration on a list of runs.
// It merges adjacent pairs of runs until 1-2 remain
func (sp *SortPlan) doMergeIteration(runs []*TempTable) []*TempTable {
	var result []*TempTable
	// Merge adjacent pairs
	for len(runs) > 1 {
		p1 := runs[0]
		p2 := runs[1]
		runs = runs[2:]
		result = append(result, sp.mergeTwoRuns(p1, p2))
	}

	// Add last run if odd number
	if len(runs) == 1 {
		result = append(result, runs[0])
	}

	return result
}

// Merges two sorted runs into a single sorted run.
func (sp *SortPlan) mergeTwoRuns(p1, p2 *TempTable) *TempTable {
	src1 := p1.Open()
	src2 := p2.Open()
	defer src1.Close()
	defer src2.Close()

	result := NewTempTable(sp.tx, sp.sch)
	dest := result.Open()
	defer dest.Close()

	hasMore1 := src1.Next()
	hasMore2 := src2.Next()

	// Merge while both runs have records
	for hasMore1 && hasMore2 {
		if sp.comp.Compare(src1, src2) < 0 {
			hasMore1 = sp.copyRecord(src1, dest)
		} else {
			hasMore2 = sp.copyRecord(src2, dest)
		}
	}

	// Copy remaining records from whichever runs has them
	for hasMore1 {
		hasMore1 = sp.copyRecord(src1, dest)
	}
	for hasMore2 {
		hasMore2 = sp.copyRecord(src2, dest)
	}

	return result
}

// Copies a record from source to destination scan
func (sp *SortPlan) copyRecord(src interfaces.Scan, dest *record.TableScan) bool {
	dest.Insert()
	for _, fieldName := range sp.sch.Fields() {
		dest.SetVal(fieldName, src.GetVal(fieldName))
	}

	return src.Next()
}
