package embedded

import "centauri/internal/app/record/schema"

// Holds metadata information about database schema
type EmbeddedMetaData struct {
	sch *schema.Schema
}

func NewEmbeddedMetaData(sch *schema.Schema) *EmbeddedMetaData {
	return &EmbeddedMetaData{
		sch: sch,
	}
}

// Returns the total number of columns in the schema
// Returns:
//   - Number of fields/columns
func (emd *EmbeddedMetaData) GetColumnCount() int {
	return len(emd.sch.Fields())
}

// Retrieves the name of specific column by its index
func (emd *EmbeddedMetaData) GetColumnName(column int) string {
	return emd.sch.Fields()[column-1]
}

// Retrieves the data type of a specific column
func (emd *EmbeddedMetaData) GetColumnType(column int) int {
	fldName := emd.GetColumnName(column)
	return int(emd.sch.DataType(fldName))
}

// Determines the display size for a specific column
// The size is determined by:
//   - For INTEGER type: fixed size of 6
//   - For other types: size specified in schema
//
// Final size is max of field name length or field data length, plus 1 for padding
func (emd *EmbeddedMetaData) GetColumnSize(column int) int {
	fldName := emd.GetColumnName(column)
	fldType := emd.sch.DataType(fldName)
	var fldLength int

	if fldType == schema.INTEGER {
		fldLength = 6
	} else {
		fldLength = emd.sch.Length(fldName)
	}

	return max(len(fldName), fldLength) + 1
}
