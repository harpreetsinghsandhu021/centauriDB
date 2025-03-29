package embedded

import (
	"centauri/internal/app/plan"
	"centauri/internal/app/server"
	"centauri/internal/app/tx"
)

// Represents a connection to an embedded CentauriDB instance.
// It maintains references to the database instance, current transaction state,
// and query planner for executing operations against the embedded database.
type EmbeddedConnection struct {
	db        *server.CentauriDB
	currentTx *tx.Transaction
	planner   *plan.Planner
}

func NewEmbeddedConnection(db *server.CentauriDB) *EmbeddedConnection {
	return &EmbeddedConnection{
		db:        db,
		currentTx: db.NewTx(),
		planner:   db.Planner(),
	}
}

// Close terminates the embedded connection and ensures all pending changes
// are committed to the database before closing. This method should be called
// when the connection is no longer needed to ensure data persistence.
func (ec *EmbeddedConnection) Close() {
	ec.commit()
}

func (ec *EmbeddedConnection) commit() {
	ec.currentTx.Commit()
	ec.currentTx = ec.db.NewTx()
}

func (ec *EmbeddedConnection) rollback() {
	ec.currentTx.Rollback()
	ec.currentTx = ec.db.NewTx()
}

func (ec *EmbeddedConnection) getTransaction() *tx.Transaction {
	return ec.currentTx
}
