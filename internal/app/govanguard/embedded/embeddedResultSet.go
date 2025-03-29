package embedded

import (
	"centauri/internal/app/interfaces"
	"centauri/internal/app/record/schema"
	"fmt"
	"strings"
)

type EmbeddedResultSet struct {
	s    interfaces.Scan
	sch  *schema.Schema
	conn *EmbeddedConnection
}

func NewEmbeddedResultSet(plan interfaces.Plan, conn *EmbeddedConnection) *EmbeddedResultSet {
	return &EmbeddedResultSet{
		s:    plan.Open(),
		sch:  plan.Schema(),
		conn: conn,
	}
}

func (ers *EmbeddedResultSet) Next() (success bool, err error) {
	// Defer recover block to catch any panics
	defer func() {
		if r := recover(); r != nil {
			success = false
			ers.conn.rollback()
			err = fmt.Errorf("panic recovered in Next(): %v", r)
		}
	}()

	// Try to execute the operation
	success = ers.s.Next()
	return success, nil
}

func (ers *EmbeddedResultSet) GetInt(fldName string) (result int, err error) {

	defer func() {
		if r := recover(); r != nil {
			result = 0
			ers.conn.rollback()
			err = fmt.Errorf("panic in GetInt for field %s: %v", fldName, r)
		}
	}()

	if ers == nil || ers.s == nil {
		return 0, fmt.Errorf("null pointer: resultSet is not initialized")
	}

	fldName = strings.ToLower(fldName)
	result = ers.s.GetInt(fldName)

	return result, nil
}

func (ers *EmbeddedResultSet) GetString(fldName string) (result string, err error) {

	defer func() {
		if r := recover(); r != nil {
			result = ""
			ers.conn.rollback()
			err = fmt.Errorf("panic in GetInt for field %s: %v", fldName, r)
		}
	}()

	if ers == nil || ers.s == nil {
		return "", fmt.Errorf("null pointer: resultSet is not initialized")
	}

	fldName = strings.ToLower(fldName)
	result = ers.s.GetString(fldName)

	return result, nil
}

func (ers *EmbeddedResultSet) GetMetaData() *EmbeddedMetaData {
	return NewEmbeddedMetaData(ers.sch)
}

func (ers *EmbeddedResultSet) Close() {
	ers.s.Close()
	ers.conn.commit()
}
