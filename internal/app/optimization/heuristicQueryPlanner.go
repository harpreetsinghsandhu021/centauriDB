package optimization

import (
	"centauri/internal/app/interfaces"
	"centauri/internal/app/metadata"
	"centauri/internal/app/parse"
	"centauri/internal/app/plan"
	"centauri/internal/app/tx"
)

// Implements the QueryPlanner interface using heuristic-based optimizaton.
// It generates left-deep query plans using various optimization stragies.
type HeuristicQueryPlanner struct {
	tablePlanners []*TablePlanner
	mdm           *metadata.MetaDataManager
}

func NewHeuristicQueryPlanner(mdm *metadata.MetaDataManager) *HeuristicQueryPlanner {
	return &HeuristicQueryPlanner{
		tablePlanners: make([]*TablePlanner, 0),
		mdm:           mdm,
	}
}

// Creates an optimized left-deep query plan for the specified query.
// It uses the following heuristics:
//   - H1: Choose the smallest table (considering selection predicates) to be first in join order.
//   - H2: Add the table to the join order which results in the smallest output
func (h *HeuristicQueryPlanner) CreatePlan(data *parse.QueryData, tx *tx.Transaction) interfaces.Plan {
	// Clear any previous table planners from prior queries
	h.tablePlanners = make([]*TablePlanner, 0)

	// Step 1: Create a TablePlanner object for each table mentioned in the query.
	// Each TablePlanner helps evaluate different access plans for that specific table.
	for _, tableName := range data.Tables() {
		// Create a TablePlanner for this table with the query's predicates
		tp := NewTablePlanner(tableName, data.Pred(), tx, h.mdm)
		h.tablePlanners = append(h.tablePlanners, tp)
	}

	// Step 2: Choose the lowest-size table plan to begin the join order
	// This implements Heuristic H1 - start with smallest table (after applying selection predicates)
	currentPlan := h.getLowestSelectPlan()

	// Step 3: Repeatedly add a plan to the join order until all tables are processed
	// This loop builds the query plan by incrementally adding tables
	for len(h.tablePlanners) > 0 {
		// First try to find the best join (if possible)
		p := h.getLowestJoinPlan(currentPlan)

		if p != nil {
			// If we found a viable join, use it
			currentPlan = p
		} else {
			// No applicate join found, use a cartesian product instead
			// Note: This is less efficient but necessary when no join conditions exist
			currentPlan = h.getLowestProductPlan(currentPlan)
		}
	}

	// Step 4: Apply projection on the desired fields and return the final plan
	// This ensures only the requested fields are returned in the query result
	return plan.NewProjectPlan(currentPlan, data.Fields())
}

// Finds the TablePlanner with the lowest expected record output after applying selection predicates,
// then removes it from the available planners.
// Steps Involved:
//   - Compares estimated record counts from each table's selection plan
//   - This is a greedy selection that optimizes for the smallest initial table
//   - The chosen TablePlanner is removed from the collection
func (h *HeuristicQueryPlanner) getLowestSelectPlan() interfaces.Plan {
	var bestTP *TablePlanner
	var bestPlan interfaces.Plan

	// Examine each table to find the one with the smallest output after selection
	for _, tp := range h.tablePlanners {
		// Get a potential plan for this table with selection predicates applied
		candidatePlan := tp.MakeSelectPlan()

		// Check if this plan is better than our current best
		// A "better" plan has fewer output records, which typically means faster processing
		if bestPlan == nil || candidatePlan.RecordsOutput() < bestPlan.RecordsOutput() {
			bestTP = tp
			bestPlan = candidatePlan
		}
	}

	// Remove the selected table planner from our collection since it's now used
	h.removeTablePlanner(bestTP)

	return bestPlan
}

// Find the tablePlanner that, when joined with the current plan,
func (h *HeuristicQueryPlanner) getLowestJoinPlan(current interfaces.Plan) interfaces.Plan {
	var bestTP *TablePlanner
	var bestPlan interfaces.Plan

	// Check each remaining table to find the best join with current plan
	for _, tp := range h.tablePlanners {
		// Try to create a join plan bw this table and our current plan
		// This may return nil if no join is possible(no common fields)
		joinPlan := tp.MakeJoinPlan(current)

		// If we found a valid join and it's better than our current best
		if joinPlan != nil && (bestPlan == nil || joinPlan.RecordsOutput() < bestPlan.RecordsOutput()) {
			bestTP = tp
			bestPlan = joinPlan
		}
	}

	// If we found at least one valid join, remove that table planner
	if bestPlan != nil {
		h.removeTablePlanner(bestTP)
	}

	return bestPlan
}

// Creates a cartesian product bw the current plan and each remaining TablePlanner, choosing the one with the
// the lowest output records. It then removes that TablePlanner from the available planners.
func (h *HeuristicQueryPlanner) getLowestProductPlan(current interfaces.Plan) interfaces.Plan {
	var bestTP *TablePlanner
	var bestPlan interfaces.Plan

	// Check each remaining table to find the best product with current plan
	for _, tp := range h.tablePlanners {
		// Create a product plan bw this table and our current plan
		productPlan := tp.MakeProductPlan(current)

		// Check if this plan is better than our current best
		if bestPlan == nil || productPlan.RecordsOutput() < bestPlan.RecordsOutput() {
			bestTP = tp
			bestPlan = productPlan
		}
	}

	// Remove the selected table planner from our collection
	h.removeTablePlanner(bestTP)

	return bestPlan
}

func (h *HeuristicQueryPlanner) removeTablePlanner(tp *TablePlanner) {
	for i, planner := range h.tablePlanners {
		if planner == tp {
			// Remove the element by slicing the slice
			// Append everything before i with everything after i
			h.tablePlanners = append(h.tablePlanners[:i], h.tablePlanners[i+1:]...)
			break
		}
	}
}
