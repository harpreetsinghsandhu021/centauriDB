package parse

import (
	"strconv"
	"strings"
	"text/scanner"
	"unicode"
)

// A Lexical analyzer for SQL Statements.
// It tokenizes SQL strings into identifiers, keywords, delimiters, and constants.
type Lexer struct {
	keywords    map[string]bool // Set of SQL keywords for quick loop
	currentRune rune            // Current token text
	scanner     scanner.Scanner // Go's built in scanner for tokenizing
}

// Creates a new lexical analyzer for SQL statement s.
func NewLexer(s string) *Lexer {
	var sc scanner.Scanner
	sc.Init(strings.NewReader(s))

	// Configure scanner
	sc.Mode = scanner.ScanIdents | scanner.ScanInts | scanner.ScanStrings

	// Allow underscores in identifiers
	// Make scanner case-sensitive for identifiers
	sc.IsIdentRune = func(ch rune, i int) bool {
		return ch == '_' || unicode.IsLetter(ch) || (i > 0 && unicode.IsDigit(ch))
	}

	lexer := &Lexer{
		scanner:  sc,
		keywords: initKeywords(),
	}

	// Read the first token
	lexer.nextToken()

	return lexer
}

// Initializes the set of SQL keywords.
// Using a map for O(1) lookup performance
func initKeywords() map[string]bool {
	keywords := map[string]bool{
		"select":  true,
		"from":    true,
		"where":   true,
		"and":     true,
		"insert":  true,
		"into":    true,
		"values":  true,
		"delete":  true,
		"update":  true,
		"set":     true,
		"create":  true,
		"table":   true,
		"int":     true,
		"varchar": true,
		"view":    true,
		"as":      true,
		"index":   true,
		"on":      true,
		"join":    true,
	}
	return keywords
}

// METHODS TO CHECK THE STATUS OF THE CURRENT TOKEN

// Returns true if the current token is the specified delimitter character.
func (l *Lexer) MatchDelim(d rune) bool {
	return l.currentRune == d
}

// Returns true if the current token is an integer.
func (l *Lexer) MatchIntConstant() bool {
	return l.currentRune == scanner.Int
}

// Returns true if the current token is a string.
func (l *Lexer) MatchStringConstant() bool {
	return l.currentRune == scanner.String
}

// Returns true if the current token is the specified keyword.
func (l *Lexer) MatchKeyword(w string) bool {
	return l.currentRune == scanner.Ident && strings.ToLower(l.scanner.TokenText()) == strings.ToLower(w)
}

// Returns true if the current token is a legal identifier.
func (l *Lexer) MatchId() bool {
	return l.currentRune == scanner.Ident && !l.keywords[strings.ToLower(l.scanner.TokenText())]
}

// METHOD TO EAT THE CURRENT TOKEN

// Throws an error if the current token is not specified delimitter.
// Otherwise moves to the next token.
func (l *Lexer) EatDelim(d rune) {
	if !l.MatchDelim(d) {
		panic("BadSyntaxException: Expected delimter " + string(d))
	}

	l.nextToken()
}

// Throws an error if the current token is not an integer.
// Otherwise, returns that integer and moves to the next token.
func (l *Lexer) EatIntConstant() int {
	if !l.MatchIntConstant() {
		panic("BadSyntaxExpection: Expected integer constant")
	}

	// Convert token to integer
	value, err := strconv.Atoi(l.scanner.TokenText())
	if err != nil {
		panic("BadSyntaxException: Invalid integer format")
	}

	l.nextToken()
	return value
}

// Throws and error if the current token is not a string.
// Otherwise, returns that string and moves to the next token.
func (l *Lexer) EatStringConstant() string {
	if !l.MatchStringConstant() {
		panic("BadSyntaxException: Expected string constant")
	}

	// Get the string value and handle quotes
	// The scanner includes the quotes, so we need to remove them
	tokenText := l.scanner.TokenText()
	value := tokenText[1 : len(tokenText)-1] // Remove surrounding quotes

	l.nextToken()
	return value
}

// Throws an error if the current token is not the specified keyword.
// Otherwise, moves to the next token.
func (l *Lexer) EatKeyword(w string) {
	if !l.MatchKeyword(w) {
		panic("BadSyntaxException: Expected keyword " + w)
	}

	l.nextToken()
}

// Throws an error if the current token is not an identifier.
// Otherwise, returns the identifier string and moves to the next token.
func (l *Lexer) EatId() string {
	if !l.MatchId() {
		panic("BadSyntaxException: Expected identifier")
	}

	value := l.scanner.TokenText()
	l.nextToken()
	return value
}

// Advances the lexer to the next token in the input stream and returns it.
// If the token is an identifier, it converts it to lowercase before storing it.
// The token text is stored in the lexer's currentText field.
// Returns the scanned token as a rune.
func (l *Lexer) nextToken() {
	l.currentRune = l.scanner.Next()
}
