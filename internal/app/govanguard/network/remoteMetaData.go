package network

import "context"

// The RMI remote interface corresponding to ResultSetMetaData.
// The methods are identical to those of ResultSetMetaData,
// except that they throw RemoteExceptions instead of SQLExceptions.
type RemoteMetaData interface {
	GetColumnCount(ctx context.Context) (int, error)
	GetColumnName(ctx context.Context, column int) (string, error)
	GetColumnType(ctx context.Context, column int) (int, error)
	GetColumnDisplaySize(ctx context.Context, column int) error
}
