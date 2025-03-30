package network

import (
	"centauri/internal/app/interfaces"
	"centauri/internal/app/record/schema"
	"context"
	"strings"
)

type RemoteResultSetServer struct {
	RemoteResultSet
	s     interfaces.Scan
	sch   *schema.Schema
	rConn *RemoteConnectionServer
}

func NewRemoteSetServer(plan interfaces.Plan, rConn *RemoteConnectionServer) (RemoteResultSet, error) {
	s := &RemoteResultSetServer{
		s:     plan.Open(),
		sch:   plan.Schema(),
		rConn: rConn,
	}
	return s, nil
}

func (rs *RemoteResultSetServer) Next(ctx context.Context) (bool, error) {
	return rs.s.Next(), nil
}
func (rs *RemoteResultSetServer) GetInt(ctx context.Context, fldName string) (int, error) {
	fldName = strings.ToLower(fldName)
	return rs.s.GetInt(fldName), nil
}
func (rs *RemoteResultSetServer) GetString(ctx context.Context, fldName string) (string, error) {
	fldName = strings.ToLower(fldName)
	return rs.s.GetString(fldName), nil
}
func (rs *RemoteResultSetServer) GetMetaData(ctx context.Context) (RemoteMetaData, error) {
	return NewRemoteMetaDataServer(rs.sch), nil
}
func (rs *RemoteResultSetServer) Close(ctx context.Context) error {
	rs.s.Close()
	rs.rConn.Commit()
	return nil
}
