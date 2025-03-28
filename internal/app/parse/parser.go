package parse

import (
	"centauri/internal/app/query"
	"centauri/internal/app/record/schema"
	"centauri/internal/app/types"
)

// Implements a recursive-descent parser for the SQL syntax.
// It converts SQL strings into structured data objects representing various SQL commands.
// Example input: "SELECT id, name FROM users WHERE age = 25"
type Parser struct {
	lexer *Lexer // The lexical analyzer that breaks input strings into tokens
}

// Creates a new parser for the given SQL string.
func NewParser(s string) *Parser {
	return &Parser{
		lexer: NewLexer(s),
	}
}

// -------- METHODS FOR PARSING PREDICATES, TERMS, EXPRESSIONS, CONSTANTS AND FIELDS --------

// Parses a database field name (an identifier)
// Returns the string representation of the field name.
// Corresponds to grammar rule: <Field> := IdTok
// Example:
//
//	In "SELECT name FROM users", "name" is a field
//	In "WHERE age = 25", "age" is a field
func (p *Parser) Field() string {
	return p.lexer.EatId()
}

// Parses a constant value (string or integer).
// Returns a Constant struct contaning the value.
// Corresponds to grammar rule: <Constant> := StrTok | IntTok
// Example: In "WHERE age = 20", "20" is an integer constant.
// Example: In "WHERE name = 'John'", "John" is a string constant.
func (p *Parser) Constant() *types.Constant {
	if p.lexer.MatchStringConstant() {
		// If the next token is a string constant, consume and wrap it
		return types.NewConstantString(p.lexer.EatStringConstant())
	} else {
		// Otherwise, assume it's an Integer constant, consume and wrap it
		return types.NewConstantInt(p.lexer.EatIntConstant())
	}
}

// Parses an expression, which can be either a field or a constant.
// Returns an Expression struct containing either a field name or a constant.
// Corresponds to grammar rule: <Expression> := <Field> | <Constant>
// Example:
//
//	In "WHERE age = 25":
//	   - "age" is a field expression
//	   - "25" is a constant expression
//	In "SELECT name FROM users":
//	   - "name" is field expression
func (p *Parser) Expression() *query.Expression {
	if p.lexer.MatchId() {
		return query.NewExpressionFieldName(p.Field())
	} else {
		return query.NewExpressionVal(p.Constant())
	}
}

// Parses a term, which is an equality comparison between two expressions.
// Returns a Term struct representing the equality comparison.
// Corresponds to grammar rule: <Term> := <Expression> = <Expression>
// Examples:
//
//	 In "WHERE age = 25":
//	     - Left expression: "age" (field)
//	     - Right expression: "25" (constant)
//	In "WHERE name = 'John'":
//	     - Left expression: "name" (field)
//	     - Right expression: "'John'" (constant)
func (p *Parser) Term() *query.Term {
	lhs := p.Expression() // Parse the left-hand side expression
	p.lexer.EatDelim('=') // Consume the equals operator
	rhs := p.Expression() // Parse the right-hand side expression

	return query.NewTerm(lhs, rhs)
}

// Parses a predicate, which is term optionally followed by "AND"
// and another predicate. Returns a Predicate struct representing the boolean condition.
// Corresponds to grammar rule: <Predicate> := <Term> [ AND <Predicate> ]
// Examples:
//   - Simple predicate: "WHERE age = 25"
//   - Compound predicate: "WHERE age = 25 AND name = 'John'"
//   - Multiple conditions: "WHERE age = 25 AND salary > 50000 AND dept = 'IT'"
func (p *Parser) Predicate() *query.Predicate {
	pred := query.NewPredicateWithTerm(p.Term()) // Start with a single term

	if p.lexer.MatchKeyword("and") {
		// If "AND" follows, consume it and recursively parse another predicate
		p.lexer.EatKeyword("and")
		// Combine the cureent predicate with the next one using AND logic
		pred.ConjoinWith(p.Predicate())
	}

	return pred
}

// -------- METHODS FOR PARSING QUERIES  ----------

// Parses a complete SELECT query with optional WHERE clause.
// Returns a QueryData struct containing fields, tables, and predicates.
// Corresponds to grammar rule: <Query> := SELECT <SelectList> FROM <TableList> [ WHERE <Predicate> ]
// Examples:
//   - Simple query, "SELECT name, age, FROM employees"
//   - With WHERE: "SELECT id, salary FROM employees WHERE dept = 'Sales'"
//   - Multiple tables: "SELECT e.name, d.location FROM employees e, departments d WHERE e.dept_id = d.id"
func (p *Parser) Query() *QueryData {
	// Parse SELECT clause
	p.lexer.EatKeyword("select")
	fields := p.SelectList()

	// Parse FROM clause
	p.lexer.EatKeyword("from")
	tables := p.TableList()

	// Parse optional WHERE clause
	pred := query.NewPredicate()

	if p.lexer.MatchKeyword("where") {
		p.lexer.EatKeyword("where")
		pred = p.Predicate()
	}

	return NewQueryData(fields, tables, pred)
}

// Parses a comma-seperated list of fields to be retrieved.
// Returns a slice of field name strings.
// Corresponds to grammar rule: <SelectList> := <Field> [ , <SelectList> ]
// Examples:
//   - Single field: "SELECT name FROM employees"
//   - Multiple fields: "SELECT id, name, salary FROM employees"
//   - ALL fields: "SELECT * FROM employees" (handled by lexer as special field)
func (p *Parser) SelectList() []string {
	var fields []string

	fields = append(fields, p.Field()) // Parse the first field

	if p.lexer.MatchDelim(',') {
		// If a comma follows, consume it an recursively parse the rest of the list
		p.lexer.EatDelim(',')
		// Append all fields from recursive call to current list
		fields = append(fields, p.SelectList()...)
	}

	return fields
}

// Parses a comma-seperated list of table names.
// Returns a slice of table name strings.
// Corresponds to grammar rule: <TableList> := IdTok [ , <TableList> ]
// Examples:
//   - Single table: "FROM employees"
//   - Multiple tables: "FROM employees, departments"
//   - With aliases: "FROM employees e, departments d"
func (p *Parser) TableList() []string {
	var tables []string
	tables = append(tables, p.lexer.EatId()) // Parse the first table name

	if p.lexer.MatchDelim(',') {
		// If a comma follows, consume it and recursively parse the rest of the list
		p.lexer.EatDelim(',')
		// Append all tables from recursive call to the current list
		tables = append(tables, p.TableList()...)
	}

	return tables
}

// -------- METHODS FOR PARSING VARIOUS UPDATE COMMANDS  ----------

// Parses any of the update commands (INSERT, DELETE, UPDATE, CREATE).
// Returns an appropriate data struct based on the command type.
// This is the main entry point for parsing all non-query SQL commands.
// Examples:
//   - "INSERT INTO users VALUES (1, 'John')" -> InsertData
//   - "DELETE FROM users WHERE id = 1" -> DeleteData
//   - "UPDATE users SET age = 30 WHERE id = 1" -> ModifyData
//   - "CREATE TABLE users (...)" -> CreateTableData
func (p *Parser) UpdateCmd() interface{} {
	if p.lexer.MatchKeyword("insert") {
		return p.Insert()
	} else if p.lexer.MatchKeyword("delete") {
		return p.Delete()
	} else if p.lexer.MatchKeyword("update") {
		return p.Modify()
	} else {
		return p.Create()
	}
}

// Parses CREATE command (TABLE, VIEW, INDEX)
// Returns appropriate data struct based on the specific create command.
// Corresponds to grammar rules fpr differnet CREATE statements.
// Examples:
//   - "CREATE TABLE users (id INT, name VARCHAR(20))"
//   - "CREATE VIEW active_users AS SELECT * FROM users WHERE status = 'active'"
//   - "CREATE INDEX idx_user_name On users(name)"
func (p *Parser) Create() interface{} {
	p.lexer.EatKeyword("create") // Consume the CREATE keyword

	if p.lexer.MatchKeyword("table") {
		// Parse a CREATE TABLE statement
		return p.CreateTable()
	} else if p.lexer.MatchKeyword("view") {
		// Parse a CREATE VIEW statement
		return p.CreateView()
	} else {
		// Assume it's a CREATE INDEX statement
		return p.CreateIndex()
	}
}

// -------- METHODS FOR PARSING DELETE COMMANDS  ----------

// Parses a DELETE command.
// Returns a DeleteData struct representing the delete operation.
// Corresponds to grammar rule: <Delete> := DELETE FROM IdTok [ WHERE <Predicate> ]
// Examples:
//   - Simple delete: "DELETE FROM users"
//   - With condition: "DELETE FROM users WHERE age < 18"
//   - Multiple conditions: "DELETE FROM users WHERE age < 18 AND status = 'inactive'"
func (p *Parser) Delete() *DeleteData {
	p.lexer.EatKeyword("delete") // Consume DELETE keyword
	p.lexer.EatKeyword("from")   // Consume FROM keyword

	tableName := p.lexer.EatId() // Parse and store the table name

	// Initialize an empty predicate (no WHERE clause)
	pred := query.NewPredicate()

	if p.lexer.MatchKeyword("where") {
		// If WHERE keyword is present, parse the predicate
		p.lexer.EatKeyword("where")
		pred = p.Predicate()
	}

	// Create and return a DeleteData object
	return NewDeleteData(tableName, pred)
}

// -------- METHODS FOR PARSING INSERT COMMANDS  ----------

// Parses an INSERT command.
// Returns an InsertData struct representing the insert operation.
// Corresponds to grammar rule: <Insert> := INSERT INTO IdTok ( <FieldList> ) VALUES ( <ConstList> )
//   - "CREATE TABLE users (id INT, name VARCHAR(20))"
//   - "CREATE VIEW active_users AS SELECT * FROM users WHERE status = 'active'"
//   - "CREATE INDEX idx_user_name ON users(name)"
func (p *Parser) Insert() *InsertData {
	p.lexer.EatKeyword("insert") // Consume INSERT keyword
	p.lexer.EatKeyword("into")   // Consume INTO keyword
	tableName := p.lexer.EatId() // Parse and store the table name

	p.lexer.EatDelim('(')   // Consume opening parenthesis
	fields := p.FieldList() // Parse the list of field names
	p.lexer.EatDelim(')')   // Consume closing parenthesis

	p.lexer.EatKeyword("values") // Consume VALUES keyword
	p.lexer.EatDelim('(')        // Consume opening parenthesis
	values := p.ConstList()      // Parse the list of constant values
	p.lexer.EatDelim(')')        // Consume closing parenthesis

	return NewInsertData(tableName, fields, values)
}

// Parses a comma-seperated list of field names.
// Returns a slice of field name strings;
// Corresponds to grammar rule: <FieldList> := <Field> [ , <FieldList> ]
// Used in INSERT statements to specify target columns.
// Examples:
//   - Single field: "(id)"
//   - Multiple fields: "(id, name, age)"
//   - With spaces: "( id , name , age )"
func (p *Parser) FieldList() []string {
	var fields []string
	fields = append(fields, p.Field()) // Parse the first field

	if p.lexer.MatchDelim(',') {
		// If a comma follows, consume it and recursively parse the rest of the list
		p.lexer.EatDelim(',')

		// Append all fields from the recursive call to the current list
		fields = append(fields, p.FieldList()...)
	}
	return fields
}

// Parses a comma-separated list of constants.
// Returns a slice of Constant structs.
// Corresponds to grammar rule: <ConstList> := <Constant> [ , <ConstList> ]
// Used in INSERT statements to specify values for insertion.
// Examples:
//   - Single integer: "(1)"
//   - Multiple types: "(1, 'John', 25)"
//   - With spaces: "( 1 , 'John' , 25 )"
func (p *Parser) ConstList() []*types.Constant {
	var constants []*types.Constant
	constants = append(constants, p.Constant()) // Parse the first constant

	if p.lexer.MatchDelim(',') {
		// If a comma follows, consume it and recursively parse th rest of the list
		p.lexer.EatDelim(',')

		// Append all constants from recursive call to the current list
		constants = append(constants, p.ConstList()...)
	}

	return constants
}

// -------- METHODS FOR PARSING MODIFY COMMANDS  ----------

// Parses an UPDATE command.
// Returns a ModifyData struct representing the update operation.
// Corresponds to grammar rule: <Modify> := UPDATE IdTok SET <Field> = <Expression> [ WHERE <Predicate> ]
// Used to modify existing records in a table.
func (p *Parser) Modify() *ModifyData {
	p.lexer.EatKeyword("update") // Consume UPDATE keyword
	tableName := p.lexer.EatId() // Parse and store the table name
	p.lexer.EatKeyword("set")    // Consume SET keyword
	fieldName := p.Field()       // Parse the field to be updated
	p.lexer.EatDelim('=')        // Consume equals operator
	newVal := p.Expression()     // Parse the new value expression

	// Initializes an empty predicate (no WHERE clause)
	pred := query.NewPredicate()

	if p.lexer.MatchKeyword("where") {
		// If WHERE keyword is present, parse the predicate
		p.lexer.EatKeyword("where")

		pred = p.Predicate()
	}

	return NewModifyData(tableName, fieldName, newVal, pred)
}

// -------- METHODS FOR PARSING CREATE TABLE COMMANDS  ----------

// Parses a CREATE TABLE command.
// Returns a CreateTableData struct representing the table creation.
// Corresponds to grammar rule: <CreateTable> := CREATE TABLE IdTok ( <FielDDefs> )
// Used to define a new table structure in the database.
func (p *Parser) CreateTable() *CreateTableData {
	p.lexer.EatKeyword("table")  // Consume TABLE keyword
	tableName := p.lexer.EatId() // Parse and store the table name
	p.lexer.EatDelim('(')        // Consume opening parenthesis
	schema := p.FieldDefs()      // Parse the field definitions into a schema
	p.lexer.EatDelim(')')        // consume closing parenthesis

	return NewCreateTableData(tableName, schema)
}

// Parses a comma-seperated list of field definitions.
// Returns a Schema struct contaning all field definitions.
// Corresponds to grammar rule: <FieldDefs> := <FieldDef> [ , <FieldDefs> ]
// Used to define multiple fields in a CREATE TABLE statement.
func (p *Parser) FieldDefs() *schema.Schema {
	schema := p.FieldDef() // Parse the first field definition

	if p.lexer.MatchDelim(',') {
		// If a comma follows, consume it and recursiovely parse the rest of the definitions
		p.lexer.EatDelim(',')
		schema2 := p.FieldDefs()

		// Merge all schemas from the recursive call into the current schema
		schema.AddAll(schema2)
	}

	return schema
}

// Parses a single field definition.
// Returns a Schema struct contanining a single field definition.
// Used to define one field with its name and type.
func (p *Parser) FieldDef() *schema.Schema {
	fieldName := p.Field() // Parse the field name
	// Continue parsing to get the field's type information
	return p.FieldType(fieldName)
}

// Parses a field type definition (int or varchar)
// Returns a Schema struct containing the field with its type.
// Corresponds to grammar rule: <TypeDef> := INT | VARCHAR (IntTok)
// Used to define the data type of a field in a CREATE TABLE statement.
func (p *Parser) FieldType(fieldName string) *schema.Schema {
	schema := schema.NewSchema() // Create a new schema to hold this field definition

	if p.lexer.MatchKeyword("int") {
		// If the type is INT, add an integer field to the schema
		p.lexer.EatKeyword("int")
		schema.AddIntField(fieldName)
	} else {
		// Otherwise, assume the type is VARCHAR with a length specification
		p.lexer.EatKeyword("varchar")
		p.lexer.EatDelim('(')
		strLen := p.lexer.EatIntConstant() // Parse the string length
		p.lexer.EatDelim(')')

		// Add a string field with the specified length to the schema
		schema.AddStringField(fieldName, strLen)
	}

	return schema
}

// -------- METHODS FOR PARSING CREATE VIEW COMMANDS  ----------

// Parses a CREATE VIEW command.
// Returns a CreateViewData struct representing the view creation.
// Corresponds to grammar rule: <CreateView> := CREATE VIEW IdTok AS <Query>
// Used to define a virtual table based on a SELECT query.
func (p *Parser) CreateView() *CreateViewData {
	p.lexer.EatKeyword("view")
	viewName := p.lexer.EatId()
	p.lexer.EatKeyword("as")
	qd := p.Query()

	return NewCreateViewData(viewName, qd)
}

// Parses a CREATE INDEX command.
// Returns a CreateIndexData struct representing the index creation.
// Corresponds to grammar rule: <CreateIndex> := CREATE INDEX IdTok ON IdTok ( <Field> )
// Used to create an index for faster query execution.
func (p *Parser) CreateIndex() *CreateIndexData {
	p.lexer.EatKeyword("index")
	indexName := p.lexer.EatId()
	p.lexer.EatKeyword("on")
	tableName := p.lexer.EatId()
	p.lexer.EatDelim('(')
	fieldName := p.Field()
	p.lexer.EatDelim(')')

	return NewCreateIndexData(indexName, tableName, fieldName)
}
