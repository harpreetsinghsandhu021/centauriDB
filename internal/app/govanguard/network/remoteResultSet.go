package network

import "context"

// The RMI remote interface corresponding to ResultSet.
// The methods are identical to those of ResultSet,
// except that they throw RemoteExceptions instead of SQLExceptions.
type RemoteResultSet interface {
	Next(ctx context.Context) (bool, error)
	GetInt(ctx context.Context, fldName string) (int, error)
	GetString(ctx context.Context, fldName string) (string, error)
	GetMetaData(ctx context.Context) (RemoteMetaData, error)
	Close(ctx context.Context) error
}
