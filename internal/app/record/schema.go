package record

// The record schema of a table.
// A schema contains the name and type of
// each field value of the table, as well as the
// length of each varchar field.
type Schema struct {
	fields []string
	info   map[string]FieldInfo
}

type FieldType int

const (
	INTEGER FieldType = 1 // integer type
	VARCHAR FieldType = 2 // string type
)

type FieldInfo struct {
	dataType FieldType
	length   int
}

func NewSchema() *Schema {
	return &Schema{
		fields: make([]string, 0),
		info:   make(map[string]FieldInfo),
	}
}

// Add a field to the schema having a specified name, type and length.
// If the field type is "integer", then the length value is irrelevant.
func (s *Schema) AddField(fieldName string, dataType FieldType, length int) {
	s.fields = append(s.fields, fieldName)
	s.info[fieldName] = FieldInfo{dataType: dataType, length: length}
}

// Adds an integer field to the schema
func (s *Schema) AddIntField(fieldName string) {
	s.AddField(fieldName, INTEGER, 0)
}

// Adds a string field to the schema.
// The length is the conceptual length of the field.
// For e.g, if the field is defined as varchar(8), then its length is 8.
func (s *Schema) AddStringField(fieldName string, length int) {
	s.AddField(fieldName, VARCHAR, length)
}

// Adds a field to the schema having the same type and length
// as the corresponding field in another schema.
func (s *Schema) Add(fieldName string, schema *Schema) {
	dataType := schema.DataType(fieldName)
	length := schema.Length(fieldName)

	s.AddField(fieldName, dataType, length)
}

// Add all of the fields in the specified schema to the current schema.
func (s *Schema) AddAll(schema *Schema) {
	for _, fieldName := range schema.Fields() {
		s.Add(fieldName, schema)
	}
}

// Returns a collection containing the name of each field in the schema.
func (s *Schema) Fields() []string {
	return s.fields
}

// Returns true if the specified field exists in the schema.
func (s *Schema) hasField(fieldname string) bool {
	for _, name := range s.fields {
		if name == fieldname {
			return true
		}
	}
	return false
}

// Returns the type of specified field.
func (s *Schema) DataType(fieldname string) FieldType {
	info, ok := s.info[fieldname]

	if !ok {
		return -1
	}

	return info.dataType
}

// Returns the conceptual length of the specified field.
// If the field is not a string field, then the return
// value is undefined.
func (s *Schema) Length(fieldname string) int {
	info, ok := s.info[fieldname]

	if !ok {
		return -1
	}

	return info.length
}
