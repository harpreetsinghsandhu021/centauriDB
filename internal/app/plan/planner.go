package plan

import (
	"centauri/internal/app/interfaces"
	"centauri/internal/app/parse"
	"centauri/internal/app/query"
	"centauri/internal/app/tx"
	"centauri/internal/app/types"
	"fmt"
	"strings"
	"unicode"
)

// Orchestrates query and update operations in the database.
// It delegates the actual execution to specialized planners while
// handling the initial parsing and validation of commands.
type Planner struct {
	qPlanner QueryPlanner  // Handles all query-related operations
	uPlanner UpdatePlanner // Handles all update-related operations
}

func NewPlanner(qPlanner QueryPlanner, uPlanner UpdatePlanner) *Planner {
	return &Planner{
		qPlanner: qPlanner,
		uPlanner: uPlanner,
	}
}

// Generates an execution plan for a query command.
// It parses the command string and delegates plan creation to the query planner.
func (p *Planner) CreateQueryPlan(cmd string, tx *tx.Transaction) interfaces.Plan {
	parser := parse.NewParser(cmd)
	data := parser.Query()
	p.verifyQuery(data)

	return p.qPlanner.CreatePlan(data, tx)
}

// Process various types of update commands.
// Returns the number of affected rows.
func (p *Planner) ExecuteUpdate(cmd string, tx *tx.Transaction) int {
	parser := parse.NewParser(cmd)
	obj := parser.UpdateCmd()

	// Verify the update command before execution
	err := p.verifyUpdate(obj)
	if err != nil {
		return 0
	}

	switch data := obj.(type) {
	case *parse.InsertData:
		return p.uPlanner.ExecuteInsert(data, tx)
	case *parse.DeleteData:
		return p.uPlanner.ExecuteDelete(data, tx)
	case *parse.ModifyData:
		return p.uPlanner.ExecuteModify(data, tx)
	case *parse.CreateTableData:
		return p.uPlanner.ExecuteCreateTable(data, tx)
	case *parse.CreateViewData:
		return p.uPlanner.ExecuteCreateView(data, tx)
	case *parse.CreateIndexData:
		return p.uPlanner.ExecuteCreateIndex(data, tx)
	default:
		return 0
	}
}

// Performs comprehensive validation of update commands.
// It validates the data structure and ensures all required fields are present
func (p *Planner) verifyUpdate(data interface{}) error {
	if data == nil {
		return fmt.Errorf("update verification failed: nil data received")
	}

	switch cmd := data.(type) {
	case *parse.InsertData:
		if err := p.verifyInsertData(cmd); err != nil {
			return fmt.Errorf("insert verification failed: %w", err)
		}
	case *parse.DeleteData:
		if err := p.verifyDeleteData(cmd); err != nil {
			return fmt.Errorf("delete verification failed: %w", err)
		}
	case *parse.ModifyData:
		if err := p.verifyModifyData(cmd); err != nil {
			return fmt.Errorf("modify verification failed: %w", err)
		}
	case *parse.CreateTableData:
		if err := p.verifyTableData(cmd); err != nil {
			return fmt.Errorf("table verification failed: %w", &err)
		}

	case *parse.CreateViewData:
		if err := p.verifyViewData(cmd); err != nil {
			return fmt.Errorf("view verification failed: %w", &err)
		}

	case *parse.CreateIndexData:
		if err := p.verifyIndexData(cmd); err != nil {
			return fmt.Errorf("view verification failed: %w", &err)
		}

	default:
		return fmt.Errorf("unknown update command type: %T", data)
	}

	return nil
}

func (p *Planner) verifyQuery(data interface{}) error {
	if data == nil {
		return fmt.Errorf("query verification failed: nil data received")
	}

	queryData, ok := data.(*parse.QueryData)

	if !ok {
		return fmt.Errorf("invalid query data type: %T", data)
	}

	if len(queryData.Fields()) > 0 {
		for _, col := range queryData.Fields() {
			if strings.TrimSpace(col) == "" {
				return fmt.Errorf("query: empty field name")
			}
		}
	}
	if len(queryData.Tables()) > 0 {
		for _, col := range queryData.Tables() {
			if strings.TrimSpace(col) == "" {
				return fmt.Errorf("query: empty table name")
			}
		}
	}

	if queryData.Pred() != nil {
		if err := p.validatePredicate(queryData.Pred()); err != nil {
			return fmt.Errorf("query: invalid predicate: %w", err)
		}
	}

	return nil
}

func (p *Planner) verifyInsertData(cmd *parse.InsertData) error {
	if cmd.TableName() == "" {
		return fmt.Errorf("missing table name")
	}

	if len(cmd.Values()) == 0 {
		return fmt.Errorf("no values provided")
	}

	// Verify column count matches values count
	if len(cmd.Fields()) > 0 && len(cmd.Fields()) != len(cmd.Values()) {
		return fmt.Errorf("column count (%d) does not match values count (%d)", len(cmd.Fields()), cmd.Values())
	}

	return nil
}

func (p *Planner) verifyDeleteData(cmd *parse.DeleteData) error {
	if cmd.TableName() == "" {
		return fmt.Errorf("missing table name")
	}

	if cmd.Pred() != nil {
		if err := p.validatePredicate(cmd.Pred()); err != nil {
			return fmt.Errorf("Invalid Predicate")

		}
	}
	return nil
}

func (p *Planner) verifyModifyData(cmd *parse.ModifyData) error {

	if cmd.TableName() == "" {
		return fmt.Errorf("missing table name")
	}

	if cmd.NewValue() == nil {
		return fmt.Errorf("no fields specified for update")
	}

	if cmd.Pred() != nil {
		if err := p.validatePredicate(cmd.Pred()); err != nil {
			return fmt.Errorf("invalid predicate: %w", err)
		}
	}

	return nil
}

func (p *Planner) verifyViewData(cmd *parse.CreateViewData) error {
	if cmd.ViewName() == "" {
		return fmt.Errorf("missing view name")
	}

	if cmd.ViewDef() == "" {
		return fmt.Errorf("missing view definition")
	}
	return nil
}

func (p *Planner) verifyTableData(cmd *parse.CreateTableData) error {

	if cmd.TableName() == "" {
		return fmt.Errorf("misssing table name")
	}

	if len(cmd.NewSchema().Fields()) == 0 {
		return fmt.Errorf("no fields defined")
	}

	return nil
}

func (p *Planner) verifyIndexData(cmd *parse.CreateIndexData) error {
	if cmd.TableName() == "" {
		return fmt.Errorf("missing index name")
	}

	if cmd.TableName() == "" {
		return fmt.Errorf("missing table name")
	}

	if cmd.FieldName() == "" {
		return fmt.Errorf("missing field name")
	}

	return nil
}

func (p *Planner) validatePredicate(pred *query.Predicate) error {
	if pred == nil {
		return fmt.Errorf("nil predicate")
	}

	if len(pred.Terms()) == 0 {
		return nil
	}

	// Validate each term in the predicate
	for i, term := range pred.Terms() {
		if err := validateTerm(&term, i); err != nil {
			return fmt.Errorf("invalid term at index %d: %w", i, err)
		}
	}

	if err := checkDuplicateTerms(pred); err != nil {
		return err
	}

	return nil
}

// Performs validation checks on a single term
func validateTerm(term *query.Term, index int) error {
	if term == nil {
		return fmt.Errorf("term is nil")
	}

	// Validate left-hand side expression
	if err := validateExpression(term.LHS(), "left-hand"); err != nil {
		return err
	}

	// Validate left-hand side expression
	if err := validateExpression(term.RHS(), "right-hand"); err != nil {
		return err
	}

	if err := validateExpressionCompatibility(term.LHS(), term.RHS()); err != nil {
		return err
	}

	return nil
}

func validateExpression(expr *query.Expression, side string) error {
	if expr == nil {
		return fmt.Errorf("%s expression is nil", side)
	}

	// If it's a field name, validate the field name
	if expr.IsFieldName() {
		if err := validateFieldName(expr.AsFieldName()); err != nil {
			return fmt.Errorf("%s field name invalid: %w", side, err)
		}
	}

	// If it's a constant, validate the constant
	if !expr.IsFieldName() {
		if err := validateConstant(expr.AsConstant()); err != nil {
			return fmt.Errorf("%s constant invalid: %w", side, err)
		}
	}

	return nil
}

// Checks if a field name follows naming conventions
func validateFieldName(name string) error {
	if name == "" {
		return fmt.Errorf("field name cannot be empty")
	}

	if len(name) > 64 {
		return fmt.Errorf("field name too long (max 64 characters)")
	}

	// First character must be a letter
	if !unicode.IsLetter(rune(name[0])) {
		return fmt.Errorf("field name must start with a letter")
	}

	// Remaining characters must be letters, numbers or underscores
	for i, ch := range name {
		if !unicode.IsLetter(ch) && !unicode.IsDigit(ch) && ch != '_' {
			return fmt.Errorf("invalid character %c at position %p in field name", ch, i)
		}
	}

	return nil
}

func validateConstant(c *types.Constant) error {
	if c == nil {
		return fmt.Errorf("constant cannot be nil")
	}

	// Check that exactly one value type is set
	if (c.AsInt() == nil && c.AsString() == nil) || (c.AsInt() != nil && c.AsString() != nil) {
		return fmt.Errorf("constant must have exactly one value type set")
	}

	return nil
}

// Checks if two expressions are type-compatible
func validateExpressionCompatibility(lhs, rhs *query.Expression) error {
	// If both are field names, no type checking needed at this stage
	if lhs.IsFieldName() && rhs.IsFieldName() {
		return nil
	}

	// If one is a field name and one is a constant, or both are constants,
	// check type compatibility

	// CONTINUE FROM HERE

	return nil
}

// Checks for duplicate terms in the predicate
func checkDuplicateTerms(p *query.Predicate) error {

	seen := make(map[string]bool)

	for _, term := range p.Terms() {
		termStr := term.String()

		if seen[termStr] {
			return fmt.Errorf("duplicate term found %s", termStr)
		}
		seen[termStr] = true
	}

	return nil
}
