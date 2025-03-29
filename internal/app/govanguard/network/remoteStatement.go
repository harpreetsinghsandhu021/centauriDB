package network

import "context"

// The RMI remote interface corresponding to Statement.
// The methods are identical to those of Statement,
// except that they are throw RemoteExceptions instead of SQLExceptions.
type RemoteStatement interface {
	ExecuteQuery(ctx context.Context, query string) (RemoteResultSet, error)
	ExecuteUpdate(ctx context.Context, cmd string) (int, error)
}
