package metadata

import (
	"centauri/internal/app/record"
	"centauri/internal/app/tx"
)

// The maximum length for string fields in the catalog table
const MAX_NAME = 16

// Manages the metadata for database tables
// It maintains the structure of catalog tables and provides methods
// for creating and accessing table information
type TableManager struct {
	tcatLayout *record.Layout // layout for table catalog
	fcatLayout *record.Layout // layout for field catalog
}

// Initializes a new TableManager
func NewTableManager(isNew bool, tx *tx.Transaction) *TableManager {
	// Define schema for the table catalog(tblcat)
	// This catalog stores information about all the tables in the database
	tcatSchema := record.NewSchema()
	tcatSchema.AddStringField("tblname", MAX_NAME) // table name
	tcatSchema.AddIntField("slotsize")             // size of each record slot in the table
	tcatLayout := record.NewLayout(tcatSchema)     // create layout from schema

	// Define schema for the field catalog (fldcat)
	// This catalog stores information about all the fields in all tables
	fcatSchema := record.NewSchema()
	fcatSchema.AddStringField("tblname", MAX_NAME) // table name, this field belongs to
	fcatSchema.AddStringField("fldname", MAX_NAME) // field name
	fcatSchema.AddIntField("type")
	fcatSchema.AddIntField("length")
	fcatSchema.AddIntField("offset")
	fcatLayout := record.NewLayout(fcatSchema)

	// If this is a new database, create the sytem catalog tables
	tm := &TableManager{
		tcatLayout: tcatLayout,
		fcatLayout: fcatLayout,
	}
	if isNew {
		tm.CreateTable("tblcat", tcatSchema, tx)
		tm.CreateTable("fldcat", fcatSchema, tx)
	}

	return tm
}

// Creates a new table in the database and registers it in the catalogs
func (tm *TableManager) CreateTable(tablename string, schema *record.Schema, tx *tx.Transaction) {
	// Create a layout for the new table based on its schema
	layout := record.NewLayout(schema)

	// Add an entry for this table in the table catalog
	tcat := record.NewTableScan(tx, "tblcat", tm.tcatLayout)
	tcat.Insert()                              // Create a new record
	tcat.SetString("tblname", tablename)       // Set the table name
	tcat.SetInt("slotsize", layout.SlotSize()) // Set the slot size
	tcat.Close()                               // Close the table scan

	// Add entries for each field in the field catalog
	fcat := record.NewTableScan(tx, "fldcat", tm.fcatLayout)

	// Iterate through all fields in the field catalog
	for _, fieldname := range schema.Fields() {
		fcat.Insert() // Create a new record for this field

		// Set field metadata
		fcat.SetString("tblname", tablename)                 // Table this field belongs to
		fcat.SetString("fldname", fieldname)                 // Field name
		fcat.SetInt("type", int(schema.DataType(fieldname))) // Data type
		fcat.SetInt("length", schema.Length(fieldname))      // Field length
		fcat.SetInt("offset", layout.Offset(fieldname))      // Field offset in record
	}

	fcat.Close()
}

// Retrieves the layout information for a specified table from the catalog
// It reads table metadata and field information from system tables
// Parameters:
//   - tablename: name of the table whose layout is being retrieved
//   - tx       : the transaction comtext for database operations
func (tm *TableManager) GetLayout(tablename string, tx *tx.Transaction) *record.Layout {
	size := -1 // Initialize the slot size to  -1, will be updated if table is found

	// Open a table scan on the table catalog ("tblcat")
	// This catalog contains metadata about all the tables in the database
	tcat := record.NewTableScan(tx, "tblcat", tm.tcatLayout)

	// Iterate through all records in the table catalog
	for tcat.Next() {
		// Check if the current record corresponds to our target table
		if tcat.GetString("tblname") == tablename {
			// Extract the slot size for this table and break
			size = tcat.GetInt("slotsize")
			break
		}
	}
	// Close when done
	tcat.Close()

	// Create a new schema object to hold field definitions
	schema := record.NewSchema()
	// Create a map to store field offsets
	offsets := make(map[string]int)

	// Open a table scan on the field catalog
	// This catalog contains metadata about all the fields in all tables
	fcat := record.NewTableScan(tx, "fldcat", tm.fcatLayout)

	// iterate through all records in the field catalog
	for fcat.Next() {
		// Check if the curremt field belongs to our target table
		if fcat.GetString("tblname") == tablename {
			// Extract field metadata
			fieldname := fcat.GetString("fldname")
			fieldType := fcat.GetInt("type")
			fieldLen := fcat.GetInt("length")
			offset := fcat.GetInt("offset")

			offsets[fieldname] = offset

			// Add the field to our schema with its type and length
			schema.AddField(fieldname, record.FieldType(fieldType), fieldLen)
		}
	}

	fcat.Close()

	// Create and return a new layout object with the collected information
	// This Layout represents the physical structure of the table
	return record.NewLayoutWithOffsets(schema, offsets, size)
}
