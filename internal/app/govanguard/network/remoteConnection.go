package network

import "context"

// The RMI remote interface corresponding to Connection.
// The methods are identical to those of Connection,
// except that they throw RemoteExceptions instead of SQLExceptions.
type RemoteConnection interface {
	CreateStatement(ctx context.Context) (RemoteStatement, error)
	Close(ctx context.Context) error
}
