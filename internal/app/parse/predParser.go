package parse

// Main parser structure for SQL predicates.
// It contains a lexer which handles tokenization of the input string.
type PredParser struct {
	lex *Lexer
}

// Creates a new predicate parser for the given input string.
// It initializes the internal lexer with the provided string.
func NewPredParser(s string) *PredParser {
	return &PredParser{
		lex: NewLexer(s),
	}
}

// Parses a field name, which must be an identifier.
// Returns the string representation of the identifier.
// Corresponds to the grammar rule: <Field> := IdTok
func (pp *PredParser) Field() string {
	return pp.lex.EatId()
}

// Parses a constant value, which can be either a string or integer.
// Corresponds to grammar rule: <Constant> : StrTok | IntTok
func (pp *PredParser) Constant() {
	if pp.lex.MatchStringConstant() {
		pp.lex.EatStringConstant() // Consume a string constant
	} else {
		pp.lex.EatIntConstant() // Consume an Integer constant
	}
}

// Parses an expression, which can be either a field or a constant.
// Corresponds to the grammar rule: <Expression> := <Field> | <Constant>
func (pp *PredParser) Expression() {
	if pp.lex.MatchId() {
		pp.Field() // Parse a field if the next token is an identifier
	} else {
		pp.Constant() // Otherwise parse a constant
	}
}

// Parses a term, which is an equality comparison between two expressions.
// Corresponds to the grammar rule: <Term> := <Expression> = <Expression>
func (pp *PredParser) Term() {
	pp.Expression()      // Parse the left-hand expression
	pp.lex.EatDelim('=') // Consume the equals delimitter
	pp.Expression()      // Parse the right-hand expression
}

// Parses a predicate, which is a term optionally followed by "AND" and another predicate.
// This implements recursive parsing to handle chained conditions.
// Corresponds to the grammar rule: <Predicate> := <Term> [ AND <Predicate> ]
func (pp *PredParser) Predicate() {
	pp.Term() // Parse the first term

	// If followed by "AND", recursively parse the rest of the predicate
	if pp.lex.MatchKeyword("and") {
		pp.lex.EatKeyword("and") // Consume the "and" keyword
		pp.Predicate()           // Recursivly parse the next predicate
	}
}
