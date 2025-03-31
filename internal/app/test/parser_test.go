package test

import (
	"centauri/internal/app/parse"
	"centauri/internal/app/query"
	"centauri/internal/app/record/schema"
	"centauri/internal/app/types"
	"reflect"
	"testing"
)

func TestParser_Query(t *testing.T) {
	tests := []struct {
		name     string
		sql      string
		expected *parse.QueryData
	}{
		{
			name: "Simple SELECT",
			sql:  "select id, name from users",
			expected: parse.NewQueryData(
				[]string{"id", "name"},
				[]string{"users"},
				query.NewPredicate(),
			),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parser := parse.NewParser(tt.sql)
			result := parser.Query()

			if !reflect.DeepEqual(result.Fields(), tt.expected.Fields()) {
				t.Errorf("Fields mismatch: got %v, want %v", result.Fields(), tt.expected.Fields())
			}

			if !reflect.DeepEqual(result.Tables(), tt.expected.Tables()) {
				t.Errorf("Tables mismatch: got %v, want %v", result.Tables(), tt.expected.Tables())
			}
		})
	}

}

func TestParser_Insert(t *testing.T) {
	tests := []struct {
		name     string
		sql      string
		expected *parse.InsertData
	}{
		{
			name: "Simple INSERT",
			sql:  "insert into users (id, name) values (1, 'John')",
			expected: parse.NewInsertData(
				"users",
				[]string{"id", "name"},
				[]*types.Constant{
					types.NewConstantInt(1),
					types.NewConstantString("John"),
				},
			),
		},
		{
			name: "INSERT with multiple columns and types",
			sql:  "insert into employees (id, name, salary, hire_date) values (100, 'Alice Kumar', 75000, '2023-01-15')",
			expected: parse.NewInsertData(
				"employees",
				[]string{"id", "name", "salary", "hire_date"},
				[]*types.Constant{
					types.NewConstantInt(100),
					types.NewConstantString("Alice Kumar"),
					types.NewConstantInt(75000),
					types.NewConstantString("2023-01-15"),
				},
			),
		},
		{
			name: "INSERT with empty string",
			sql:  "insert into notes (id, title, content) values (1, '', 'Some content')",
			expected: parse.NewInsertData(
				"notes",
				[]string{"id", "title", "content"},
				[]*types.Constant{
					types.NewConstantInt(1),
					types.NewConstantString(""),
					types.NewConstantString("Some content"),
				},
			),
		},
		// TODO: Implement support for NULL values
		// {
		// 	name: "INSERT with NULL values",
		// 	sql:  "insert into products (id, name, description, price) values (1, 'Widget', NULL, 29)",
		// 	expected: parse.NewInsertData(
		// 		"products",
		// 		[]string{"id", "name", "description", "price"},
		// 		[]*types.Constant{
		// 			types.NewConstantInt(1),
		// 			types.NewConstantString("Widget"),
		// 			types.NewConstantString("NULL"),
		// 			types.NewConstantInt(29),
		// 		},
		// 	),
		// },
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parser := parse.NewParser(tt.sql)
			result := parser.Insert()

			if result.TableName() != tt.expected.TableName() {
				t.Errorf("Table name mismatch: got %v, want %v", result.TableName(), tt.expected.TableName())
			}

			if !reflect.DeepEqual(result.Fields(), tt.expected.Fields()) {
				t.Errorf("Fields mismatch: got %v, want %v", result.Fields(), tt.expected.Fields())
			}

			if !reflect.DeepEqual(result.Values(), tt.expected.Values()) {
				t.Errorf("Values mismatch: got %v, want %v", result.Values(), tt.expected.Values())
			}
		})
	}

}

func TestParser_Delete(t *testing.T) {
	tests := []struct {
		name     string
		sql      string
		expected *parse.DeleteData
	}{
		{
			name: "Simple DELETE",
			sql:  "delete from users where id = 1",
			expected: parse.NewDeleteData("users",
				query.NewPredicateWithTerm(
					query.NewTerm(
						query.NewExpressionFieldName("id"),
						query.NewExpressionVal(types.NewConstantInt(1)),
					),
				)),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parser := parse.NewParser(tt.sql)
			result := parser.Delete()

			if result.TableName() != tt.expected.TableName() {
				t.Errorf("Table name mismatch: got %v, want %v", result.TableName(), tt.expected.TableName())
			}

			if !reflect.DeepEqual(result.Pred(), tt.expected.Pred()) {
				t.Errorf("Predicate mismatch: got %v, want %v", result.Pred(), tt.expected.Pred())
			}
		})
	}

}

func TestParser_CreateTable(t *testing.T) {
	tests := []struct {
		name     string
		sql      string
		expected *parse.CreateTableData
	}{
		{
			name: "Create table with multiple fields",
			sql:  "table users (id int, name varchar(20))",
			expected: func() *parse.CreateTableData {
				s := schema.NewSchema()
				s.AddIntField("id")
				s.AddStringField("name", 20)
				return parse.NewCreateTableData("users", s)
			}(),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parser := parse.NewParser(tt.sql)
			result := parser.CreateTable()

			if result.TableName() != tt.expected.TableName() {
				t.Errorf("Table name mismatch: got %v, want %v", result.TableName(), tt.expected.TableName())
			}

			if !reflect.DeepEqual(result.NewSchema().Fields(), tt.expected.NewSchema().Fields()) {
				t.Errorf("Table Schema mismatch: got %v, want %v", result.NewSchema().Fields(), tt.expected.NewSchema().Fields())

			}
		})
	}

}

func TestParser_UpdateCmd(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		kind string
	}{
		{
			name: "INSERT command",
			sql:  "insert into users (id, name) values (1, 'John')",
			kind: "*parse.InsertData",
		},
		{
			name: "DELETE command",
			sql:  "delete from users where id = 1",
			kind: "*parse.DeleteData",
		},
		{
			name: "CREATE command",
			sql:  "create table users (id int, name varchar(20))",
			kind: "*parse.CreateTableData",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parser := parse.NewParser(tt.sql)
			result := parser.UpdateCmd()

			resultType := reflect.TypeOf(result).String()
			if resultType != tt.kind {
				t.Errorf("Command type mismatch: got %v, want %v", resultType, tt.kind)
			}
		})
	}

}
