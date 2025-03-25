package parse

import "testing"

// Extends SQLParser to record method calls for testing
type TracingSQLParser struct {
	SQLParser
	Trace []string
}

// Creates a new tracing parser with the given lexer
func NewTracingSQLParser(lexer *Lexer) *TracingSQLParser {
	return &TracingSQLParser{
		SQLParser: SQLParser{lex: lexer},
		Trace:     []string{},
	}
}

// OVERRIDE METHODS TO ADD TRACING

func (tsp *TracingSQLParser) Field() string {
	tsp.Trace = append(tsp.Trace, "Field")
	return tsp.SQLParser.Field()
}

func (tsp *TracingSQLParser) Constant() string {
	tsp.Trace = append(tsp.Trace, "Constant")
	return tsp.SQLParser.Constant()
}

func (tsp *TracingSQLParser) Expression() string {
	tsp.Trace = append(tsp.Trace, "Expression")
	return tsp.SQLParser.Expression()
}

func (tsp *TracingSQLParser) ComparisonOperator() string {
	tsp.Trace = append(tsp.Trace, "ComparisonOperator")
	return tsp.SQLParser.ComparisonOperator()
}

func (tsp *TracingSQLParser) Term() {
	tsp.Trace = append(tsp.Trace, "Term")
	tsp.SQLParser.Term()
}

func (tsp *TracingSQLParser) Predicate() {
	tsp.Trace = append(tsp.Trace, "Predicate")
	tsp.SQLParser.Predicate()
}

func (tsp *TracingSQLParser) TableRef() (string, string) {
	tsp.Trace = append(tsp.Trace, "TableRef")
	return tsp.SQLParser.TableRef()
}

func (tsp *TracingSQLParser) JoinCondition() {
	tsp.Trace = append(tsp.Trace, "JoinCondition")
	tsp.SQLParser.JoinCondition()
}

func (tsp *TracingSQLParser) JoinType() string {
	tsp.Trace = append(tsp.Trace, "JoinType")
	return tsp.SQLParser.JoinType()
}

func (tsp *TracingSQLParser) Join() {
	tsp.Trace = append(tsp.Trace, "Join")
	tsp.SQLParser.Join()
}

func (tsp *TracingSQLParser) FromClause() {
	tsp.Trace = append(tsp.Trace, "FromClause")
	tsp.SQLParser.FromClause()
}

func (tsp *TracingSQLParser) WhereClause() {
	tsp.Trace = append(tsp.Trace, "WhereClause")
	tsp.SQLParser.WhereClause()
}

func (tsp *TracingSQLParser) SelectItem() {
	tsp.Trace = append(tsp.Trace, "SelectItem")
	tsp.SQLParser.SelectItem()
}

func (tsp *TracingSQLParser) SelectList() {
	tsp.Trace = append(tsp.Trace, "SelectList")
	tsp.SQLParser.SelectList()
}

func (tsp *TracingSQLParser) Query() {
	tsp.Trace = append(tsp.Trace, "Query")
	tsp.SQLParser.JoinType()
}

// Helper to check if a trace includes the expected method calls in order
func verifyTrace(t *testing.T, trace []string, expected []string) {
	pass := true

	// First, check if all the expected methods were called
	expectedIndex := 0

	for _, method := range trace {
		if expectedIndex < len(expected) && method == expected[expectedIndex] {
			expectedIndex++
		}
	}

	if expectedIndex < len(expected) {
		pass = false
		t.Errorf("Missing expected method calls. Found %v but expected %v", trace, expected)
	}

	// Print the trace for debugging
	if !pass {
		t.Logf("Method call trace: %v", trace)
	}
}

// Test Basic field parsing with qualified names
func TestField(t *testing.T) {
	// Test 1: Simple field
	lexer := NewLexer("column")
	parser := NewTracingSQLParser(lexer)

	result := parser.Field()
	if result != "column" {
		t.Errorf("Expected 'column', got '%s'", result)
	}

	// Test 2: Qualified field
	lexer = NewLexer("table.column")
	parser = NewTracingSQLParser(lexer)

	result = parser.Field()
	if result != "table.column" {
		t.Errorf("Expected 'table.column', got '%s'", result)
	}
}
