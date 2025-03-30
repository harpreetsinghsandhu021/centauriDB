package network

import (
	"centauri/internal/app/server"
	"context"
)

type DriverServer struct {
	RemoteDriver
	db *server.CentauriDB
}

func NewDriverServer(db *server.CentauriDB) (*DriverServer, error) {
	return &DriverServer{db: db}, nil
}

func (d *DriverServer) Connect(ctx context.Context) (RemoteConnection, error) {
	return NewRemoteConnectionServer(d.db)
}
