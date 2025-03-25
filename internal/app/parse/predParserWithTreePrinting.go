// Exercise 9.5

package parse

import (
	"fmt"
	"strings"
)

// Refined parser structure for SQL predicates.
// It contains a lexer which handles tokeniztion of the input string.
// Added an indentation level to track the depth of the parse tree.
type RefinedPredParser struct {
	lex           *Lexer
	indentLevel   int
	parseTreeLogs []string
}

func NewRefinedPredParser(s string) *RefinedPredParser {
	return &RefinedPredParser{
		lex:           NewLexer(s),
		indentLevel:   0,
		parseTreeLogs: []string{},
	}
}

// Helper method to add indentation based on current parse depth
func (rpp *RefinedPredParser) indent() string {
	return strings.Repeat(" ", rpp.indentLevel)
}

// Helper method to log a node in the parse tree
func (rpp *RefinedPredParser) logNode(nodeName string, value string) {
	nodeInfo := fmt.Sprintf("%s%s: %s", rpp.indent(), nodeName, value)
	rpp.parseTreeLogs = append(rpp.parseTreeLogs, nodeInfo)
}

// Helper method to start a new parse branch
func (rpp *RefinedPredParser) enterNode(nodeName string) {
	rpp.logNode(nodeName, "")
	rpp.indentLevel++
}

func (rpp *RefinedPredParser) exitNode() {
	rpp.indentLevel--
}

// Parses a field name, which must be an identifier.
// Returns the string reprsentation of the identifier.
// Corresponds to the grammar rule: Field := IdTok
func (rpp *RefinedPredParser) Field() string {
	rpp.enterNode("Field")
	id := rpp.lex.EatId()
	rpp.logNode("Identifier", id)
	rpp.exitNode()
	return id
}

// Parses a constant value, which can be either a string or integer.
// Returns the constant value as a string.
// Corresponds to grammar rule: Constant: StrTok | IntTok
func (rpp *RefinedPredParser) Constant() string {
	rpp.enterNode("Constant")
	var value string

	if rpp.lex.MatchStringConstant() {
		value = rpp.lex.EatStringConstant() // Consume a string constant
		rpp.logNode("StringConstant", value)
	} else {
		value = fmt.Sprintf("%d", rpp.lex.EatIntConstant()) // Consume an integer constant
		rpp.logNode("IntConstant", value)
	}

	rpp.exitNode()
	return value
}

func (rpp *RefinedPredParser) Expression() string {
	rpp.enterNode("Expression")
	var value string

	if rpp.lex.MatchId() {
		value = rpp.Field() // Parse a field if the next token is an idenitifier
	} else {
		value = rpp.Constant() // Otherwise parse a constant
	}

	rpp.exitNode()
	return value
}

func (rpp *RefinedPredParser) Term() {
	rpp.enterNode("Term")
	leftExpr := rpp.Expression()
	rpp.lex.EatDelim('=')
	rpp.logNode("Operator", "=")
	rightExpr := rpp.Expression()
	rpp.logNode("Comparison", fmt.Sprintf("%s = %s", leftExpr, rightExpr))
	rpp.exitNode()
}

func (rpp *RefinedPredParser) Predicate() {
	rpp.enterNode("Predicate")
	rpp.Term()

	if rpp.lex.MatchKeyword("and") {
		rpp.lex.EatKeyword("and")
		rpp.logNode("Connector", "AND")
		rpp.Predicate()
	}
	rpp.exitNode()
}

// Prints the entire tree after parsing is complete
func (rpp *RefinedPredParser) PrintParseTree() {
	fmt.Println("Parse Tree:")
	for _, line := range rpp.parseTreeLogs {
		fmt.Println(line)
	}
}
