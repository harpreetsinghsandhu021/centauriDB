package network

import (
	"centauri/internal/app/plan"
	"context"
	"fmt"
	"runtime/debug"
)

type RemoteStatementServer struct {
	RemoteStatement
	rConn   *RemoteConnectionServer
	planner *plan.Planner
}

func NewRemoteStatementServer(c *RemoteConnectionServer, p *plan.Planner) (RemoteStatement, error) {
	rss := &RemoteStatementServer{
		rConn:   c,
		planner: p,
	}
	return rss, nil
}

func (rss *RemoteStatementServer) ExecuteQuery(ctx context.Context, query string) (result RemoteResultSet, err error) {
	// Defer recovery function to handle panics
	defer func() {
		if r := recover(); r != nil {
			// Convert panic to error
			switch x := r.(type) {
			case string:
				err = fmt.Errorf("panic in ExecuteQuery: %s", x)
			case error:
				err = fmt.Errorf("panic in ExecuteQuery: %w", x)
			default:
				err = fmt.Errorf("panic in ExecuteQuery: %v", x)
			}

			// Optionally log the stack trace
			debug.PrintStack()

			rss.rConn.Rollback()

			// Ensure result is nil in case of panic
			result = nil
		}
	}()

	tx := rss.rConn.GetTransaction()
	plan := rss.planner.CreateQueryPlan(query, tx)
	return NewRemoteSetServer(plan, rss.rConn)
}

func (rss *RemoteStatementServer) ExecuteUpdate(ctx context.Context, cmd string) (int, error) {
	tx := rss.rConn.GetTransaction()
	result := rss.planner.ExecuteUpdate(cmd, tx)
	rss.rConn.Commit()

	return result, nil
}
