package network

import (
	"centauri/internal/app/plan"
	"centauri/internal/app/server"
	"centauri/internal/app/tx"
	"context"
)

type RemoteConnectionServer struct {
	RemoteConnection
	db        *server.CentauriDB
	currentTx *tx.Transaction
	planner   *plan.Planner
}

func NewRemoteConnectionServer(db *server.CentauriDB) (RemoteConnection, error) {
	conn := &RemoteConnectionServer{
		db:        db,
		currentTx: db.NewTx(),
		planner:   db.Planner(),
	}

	return conn, nil
}

func (c *RemoteConnectionServer) CreateStatement(ctx context.Context) (RemoteStatement, error) {
	return NewRemoteStatementServer(c, c.planner)
}

func (c *RemoteConnectionServer) Close(ctx context.Context) error {
	c.currentTx.Commit()
	return nil
}

func (c *RemoteConnectionServer) GetTransaction() *tx.Transaction {
	return c.currentTx
}

func (c *RemoteConnectionServer) Commit() {
	c.currentTx.Commit()
	c.currentTx = c.db.NewTx()
}

func (c *RemoteConnectionServer) Rollback() {
	c.currentTx.Rollback()
	c.currentTx = c.db.NewTx()
}
