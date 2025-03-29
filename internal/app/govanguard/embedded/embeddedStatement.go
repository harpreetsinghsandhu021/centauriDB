package embedded

import "centauri/internal/app/plan"

// Represents a statement in the embedded database context.
// It holds a reference to the embedded connection and query planner,
// allowing for execution of SQL statements in an embedded database environment.
type EmbeddedStatement struct {
	conn    *EmbeddedConnection
	planner *plan.Planner
}

func NewEmbeddedStatement(conn *EmbeddedConnection, planner *plan.Planner) *EmbeddedStatement {
	return &EmbeddedStatement{
		conn:    conn,
		planner: planner,
	}
}

// Executes a query and returns a result set
func (es *EmbeddedStatement) ExecuteQuery(query string) *EmbeddedResultSet {
	tx := es.conn.getTransaction()
	plan := es.planner.CreateQueryPlan(query, tx)
	return NewEmbeddedResultSet(plan, es.conn)
}

// Executes an update command and returns the number of affected rows
func (es *EmbeddedStatement) ExecuteUpdate(cmd string) (int, error) {
	// Get the transaction from the connection
	tx := es.conn.getTransaction()

	// Execute the update
	result := es.planner.ExecuteUpdate(cmd, tx)

	// Commit the transaction
	es.conn.commit()

	return result, nil
}

// Closes the statement
func (es *EmbeddedStatement) Close() error {
	return nil
}
