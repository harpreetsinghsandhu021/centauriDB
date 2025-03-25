// Exercise 9.10
// Revised Version of PredParser to handle explicit joins
package parse

// Main parser structure for SQL queries.
// Extended to support JOIN operations and more complex SQL constructs.
type SQLParser struct {
	lex *Lexer
}

func NewSQLParser(s string) *SQLParser {
	return &SQLParser{
		lex: NewLexer(s),
	}
}

// Parses a field name, which must be an identifier.
// Now supports qualified column names in the form "table.column"
// Returns the string representation of the identifier.
func (sp *SQLParser) Field() string {
	// Parse the first identifier (table name or column name)
	id := sp.lex.EatId()

	// If followed by a dot, this is a qualified column name
	if sp.lex.MatchDelim('.') {
		// Consume the dot
		sp.lex.EatDelim('.')
		// Parse the column name that follows the table name
		columnName := sp.lex.EatId()
		// Return the qualified name in the form "table.column"
		return id + "." + columnName
	}

	return id
}

// Parses a constant value, which can be either a string or integer.
// Returns the constant value as a strinf for consistency.
func (sp *SQLParser) Constant() string {
	if sp.lex.MatchStringConstant() {
		return sp.lex.EatStringConstant()
	} else {
		return string(sp.lex.EatIntConstant())
	}
}

// Parses an expression, which can be a field or a constant.
func (sp *SQLParser) Expression() string {
	// parse a field if next token is an identifier
	if sp.lex.MatchId() {
		return sp.Field()
	} else {
		return sp.Constant()
	}
}

// Parses a comparison operator, which can be =, <>, <, >, <= or >=
// Returns the string representation of the operator.
// Corresponds to grammar rule: <CompOp> := = | <> | < | > | <= | >=
func (sp *SQLParser) ComparisonOperator() string {
	if sp.lex.MatchDelim('=') {
		sp.lex.EatDelim('=')
		return "="
	} else if sp.lex.MatchDelim('<') {
		sp.lex.EatDelim('<')
		if sp.lex.MatchDelim('>') {
			sp.lex.EatDelim('>')
			return "<>"
		} else if sp.lex.MatchDelim('=') {
			sp.lex.EatDelim('=')
			return "<="
		}

		return "<"
	} else if sp.lex.MatchDelim('>') {
		sp.lex.EatDelim('>')

		if sp.lex.MatchDelim('=') {
			sp.lex.EatDelim('=')
			return ">="
		}
		return ">"
	}

	// If no comparision operator is found, default to equals
	// This is for backward compatibility, but ideally should throw an error
	sp.lex.EatDelim('=')
	return "="
}

// Parses a term, which is a comparison between two expressions.
// Now supports different comparison operators.
// Corresponds to the grammar rule: <Term> := <Expression> <CompOp> <Expression>
func (sp *SQLParser) Term() {
	sp.Expression()         // Parse the left-hand expression
	sp.ComparisonOperator() // Parse the comparison operator
	sp.Expression()         // Parse the right-hand expression
}

// Parses a predicate, which is a term optionally followed by a logical connector and logical predicate
// Extended to support botht AND and OR logical operators.
func (sp *SQLParser) Predicate() {
	sp.Term()

	// Check for logical Operators (AND or OR)
	if sp.lex.MatchKeyword("and") {
		sp.lex.EatKeyword("and")
		sp.Predicate()
	} else if sp.lex.MatchKeyword("or") {
		sp.lex.EatKeyword("or")
		sp.Predicate()
	}
}

// Parses a table reference, which can be a simple table name or a table with an alias.
// Returns the table name and alias(if any)
// Corresponds to the grammar rule: <TableRef> := IdTok [ AS IdTok ]
func (sp *SQLParser) TableRef() (string, string) {
	// Parse the table name
	tableName := sp.lex.EatId()

	// Check if there's an alias using the AS keyword
	if sp.lex.MatchKeyword("as") {
		sp.lex.EatKeyword("as")
		alias := sp.lex.EatId()
		return tableName, alias
	}

	// Check if there's an alias without the AS keyword
	if sp.lex.MatchId() {
		alias := sp.lex.EatId()
		return tableName, alias
	}

	return tableName, ""
}

// Parses a join condition, which follows the ON keyword in a JOIN clause.
// Corresponds to the grammar rule: <JoinCondition> := ON <Predicate>
func (sp *SQLParser) JoinCondition() {
	sp.lex.EatKeyword("on")
	sp.Predicate()
}

// Parses a join type (INNER, RIGHT, LEFT, FULL).
// Returns the join type as a string.
// Corresponds to the grammar rule: <JoinType> := [ INNER | LEFT [OUTER] | RIGHT [OUTER] | FULL [OUTER] ]
func (sp *SQLParser) JoinType() string {
	if sp.lex.MatchKeyword("inner") {
		sp.lex.EatKeyword("inner")
		return "INNER"
	} else if sp.lex.MatchKeyword("left") {
		sp.lex.EatKeyword("left")
		// Check for optional OUTER keyword
		if sp.lex.MatchKeyword("outer") {
			sp.lex.EatKeyword("outer")
		}
		return "LEFT"
	} else if sp.lex.MatchKeyword("right") {
		sp.lex.EatKeyword("right")
		// Check for optional OUTER keyword
		if sp.lex.MatchKeyword("outer") {
			sp.lex.EatKeyword("outer")
		}
		return "RIGHT"
	} else if sp.lex.MatchKeyword("full") {
		sp.lex.EatKeyword("full")

		if sp.lex.MatchKeyword("outer") {
			sp.lex.EatKeyword("outer")
		}

		return "FULL"
	}

	// If no explicit join type is specified, default to INNER JOIN
	return "INNER"
}

// Parses a join clause, which consists of a join type, a table reference, and a join condition.
// Corresponds to the grammar rule: <Join> := <JoinType> JOIN <TableRef> <JoinCondition>
func (sp *SQLParser) Join() {
	sp.JoinType()             // Parse the join type (INNER, LEFT, RIGHT, FULL)
	sp.lex.EatKeyword("join") // Expect the JOIN keyword
	sp.TableRef()             // Parse the table being joined
	sp.JoinCondition()        // Parse the join condition
}

// Parses a from clause, which specifies the tables involved in the query.
// Now supports multiple tables with explicit JOIN Operations.
// Corresponds to the grammar rule: <FromClause> := FROM <TableRef> { <Join> }
func (sp *SQLParser) FromClause() {
	// Expect the FROM Keyword
	sp.lex.EatKeyword("from")
	// Parse the first table reference
	sp.TableRef()

	// Parse any subsequent JOIN clauses
	for sp.lex.MatchKeyword("inner") || sp.lex.MatchKeyword("left") ||
		sp.lex.MatchKeyword("right") || sp.lex.MatchKeyword("full") || sp.lex.MatchKeyword("join") {
		// If "JOIN" appears without a preceding join type, it's an INNER JOIN
		if sp.lex.MatchKeyword("join") {
			sp.lex.EatKeyword("join")
			sp.TableRef()
			sp.JoinCondition()
		} else {
			// Parse a join with an explicit type
			sp.Join()
		}
	}
}

// Parses a where clause, which filters the query result.
// Corresponds to the grammar rule: <WhereCluase> : WHERE <Predicate>
func (sp *SQLParser) WhereClause() {
	sp.lex.EatKeyword("where")
	sp.Predicate()
}

// Parses a select item, which can be a column, an expression, or a wildcard (*).
// Corresponds to the grammar rule: <SelectItem> := <Expression> [ AS IdTok ] | *
func (sp *SQLParser) SelectItem() {
	if sp.lex.MatchDelim('*') {
		sp.lex.EatDelim('*')
		return
	}

	// Parse an expression
	sp.Expression()

	// Check for an alias using the AS keyword
	if sp.lex.MatchKeyword("as") {
		sp.lex.EatKeyword("as")
		sp.lex.EatId()
	} else if sp.lex.MatchId() {
		// Check for an alias without the AS keyword
		sp.lex.EatId()
	}
}

// Parses a list of select items seperated by commaas
func (sp *SQLParser) SelectList() {
	sp.SelectItem()

	for sp.lex.MatchDelim(',') {
		sp.lex.EatDelim(',')
		sp.SelectItem()
	}
}

// Parses a complete SELECT statement with support for joins.
// Corresponds to the grammar rule:
// <Query> := SELECT <SelectList> <FromClause> [ <WhereClause> ]
func (sp *SQLParser) Query() {
	sp.lex.EatKeyword("select")
	sp.SelectList()

	// Parse the from clause with potentital joins
	sp.FromClause()

	// Parse an optional where clause
	if sp.lex.MatchKeyword("where") {
		sp.WhereClause()
	}
}
