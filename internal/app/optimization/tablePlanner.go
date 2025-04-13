package optimization

import (
	"centauri/internal/app/index/planner"
	"centauri/internal/app/interfaces"
	"centauri/internal/app/metadata"
	"centauri/internal/app/multibuffer"
	"centauri/internal/app/plan"
	"centauri/internal/app/query"
	"centauri/internal/app/record/schema"
	"centauri/internal/app/tx"
	"fmt"
)

// Contains methods for planning operations on a single table. It evaluates different access paths for a
// table and determines the optimal plan based on available indexes and predicate conditions.
type TablePlanner struct {
	myplan   *plan.TablePlan
	mypred   *query.Predicate
	myschema *schema.Schema
	indexes  map[string]metadata.IndexInfo
	tx       *tx.Transaction
}

func NewTablePlanner(tableName string, mypred *query.Predicate, tx *tx.Transaction, mdm *metadata.MetaDataManager) *TablePlanner {
	tablePlan := plan.NewTablePlan(tx, tableName, mdm).(*plan.TablePlan)

	return &TablePlanner{
		myplan:   tablePlan,
		mypred:   mypred,
		tx:       tx,
		myschema: tablePlan.Schema(),
		indexes:  mdm.GetIndexInfo(tableName, tx),
	}
}

// Constructs a select plan for the table.
// The plan will use an IndexSelect if possible, which can be significantly more efficient than scanning
// the entire tabel when an appropriate index exists.
func (tp *TablePlanner) MakeSelectPlan() interfaces.Plan {
	// First try to use an index if possible
	p := tp.makeIndexSelect()
	// If no applicable index found, use the basic table plan
	if p == nil {
		p = tp.myplan
	}

	// Add any applicable selection predicates
	return tp.addSelectPred(p)
}

// Constructs a join plan b/w the specified plan and this table.
// The plan will use an IndexJoin if possible, which is typically more efficient than
// a product join. If no join is possible (no join predicates exist bw the tables), the method returns nil
func (tp *TablePlanner) MakeJoinPlan(current interfaces.Plan) interfaces.Plan {
	// Get the schema of the current plan
	currsch := current.Schema()
	// Find predicates that join the current plan with this table
	joinpred := tp.mypred.JoinSubPred(tp.myschema, currsch)
	// If no predicates exist, return nil
	if joinpred == nil {
		return nil
	}

	// Try to create an index join if possible
	p := tp.makeIndexJoin(current, currsch)

	// If no index join is possible, fall back to a product join
	if p == nil {
		p = tp.makeProductJoin(current, currsch)
	}

	return p
}

// Constructs a product plan b/w the specified plan and this table.
// This is used when there are no join conditions or as a fallback when index joins are not possible.
func (tp *TablePlanner) MakeProductPlan(current interfaces.Plan) interfaces.Plan {
	//  First add any selection predicates to the table plan
	p := tp.addSelectPred(tp.myplan)

	return multibuffer.NewMultiBufferProductPlan(tp.tx, current, p)
}

// Creates an index select plan if there's an index on a field that is used
// in an equality condition with a constant.
func (tp *TablePlanner) makeIndexSelect() interfaces.Plan {
	for fieldName := range tp.indexes {
		val := tp.mypred.EquatesWithConstant(fieldName)

		// If we found an equality condition with a constant
		if val != nil {
			ii := tp.indexes[fieldName]
			fmt.Println("index on", fieldName, "used")

			return planner.NewIndexSelectPlan(tp.myplan, &ii, *val)
		}
	}

	// No applicable index found
	return nil
}

// Creates an index join plan if there's an index on a field in this table that is used in an
// equality condition witht a field from the outer plan.
func (tp *TablePlanner) makeIndexJoin(current interfaces.Plan, currsch *schema.Schema) interfaces.Plan {
	for fieldName := range tp.indexes {
		// See if the predicate equates this fiels with a field in the outer plan
		outerField := tp.mypred.EquatesWithField(fieldName)

		// If we found a matching field in the outer plan
		if outerField != "" && currsch.HasField(outerField) {
			ii := tp.indexes[fieldName]
			p := planner.NewIndexJoinPlan(current, tp.myplan, &ii, outerField)
			p = tp.addSelectPred(p)

			return tp.addJoinPred(p, currsch)
		}
	}

	return nil
}

// Creates a product join plan when an index join is not possible.
// It applies all relevant join predicates after performing the product.
func (tp *TablePlanner) makeProductJoin(current interfaces.Plan, currsch *schema.Schema) interfaces.Plan {
	p := tp.MakeProductPlan(current)

	return tp.addJoinPred(p, currsch)
}

// Adds a selection plan on top of the specified plan
// if there are any applicable selection predicates.
func (tp *TablePlanner) addSelectPred(p interfaces.Plan) interfaces.Plan {
	// Extract the portion of the predicate that applies only to this table
	selectPred := tp.mypred.SelectSubPred(tp.myschema)

	if selectPred != nil {
		return plan.NewSelectPlan(p, selectPred)
	}

	return p
}

// Adds a selection plan on top of the specified plan to handle
// join predicates that couldn't be implemented as an index join.
func (tp *TablePlanner) addJoinPred(p interfaces.Plan, currsch *schema.Schema) interfaces.Plan {
	// Extract join predicates b/w the current schema and this table's schema
	joinpred := tp.mypred.JoinSubPred(currsch, tp.myschema)

	if joinpred != nil {
		return plan.NewSelectPlan(p, joinpred)
	}

	return p
}
