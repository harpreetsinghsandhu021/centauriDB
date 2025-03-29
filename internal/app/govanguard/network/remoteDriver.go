package network

import "context"

// The RMI(Remote Method Invocation) remote interface corresponding to Driver.
// The method is similar to that of Driver, except that it takes no arguments
// and throws RemoteExceptions instead of SQL exceptions.
type RemoteDriver interface {
	Connect(ctx context.Context) (RemoteConnection, error)
}
