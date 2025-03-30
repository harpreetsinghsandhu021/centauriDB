package network

import (
	"centauri/internal/app/record/schema"
	"context"
	"fmt"
)

type RemoteMetaDataServer struct {
	RemoteMetaData
	sch    *schema.Schema
	fields []string
}

func NewRemoteMetaDataServer(sch *schema.Schema) RemoteMetaData {
	rmdServer := &RemoteMetaDataServer{
		sch: sch,
	}
	for _, field := range sch.Fields() {
		rmdServer.fields = append(rmdServer.fields, field)
	}

	return rmdServer
}

func (s *RemoteMetaDataServer) GetColumnCount(ctx context.Context) (int, error) {
	return len(s.fields), nil
}

func (s *RemoteMetaDataServer) GetColumnName(ctx context.Context, column int) (string, error) {
	return s.fields[column-1], nil
}

func (s *RemoteMetaDataServer) GetColumnType(ctx context.Context, column int) (int, error) {
	fldName, err := s.GetColumnName(ctx, column)
	if err != nil {
		return 0, fmt.Errorf("error when getting column name: %w", err)
	}

	return int(s.sch.DataType(fldName)), nil
}

func (s *RemoteMetaDataServer) GetColumnDisplaySize(ctx context.Context, column int) (int, error) {
	fldName, err := s.GetColumnName(ctx, column)
	if err != nil {
		return 0, fmt.Errorf("error when getting column name: %w", err)
	}
	fldType := s.sch.DataType(fldName)
	fldLength := 0

	if fldType == schema.INTEGER {
		fldLength = 6
	} else {
		fldLength = s.sch.Length(fldName)
	}

	return max(len(fldName), fldLength) + 1, nil

}
