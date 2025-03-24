package parse

type CreateIndexData struct {
	idxName   string
	tableName string
	fieldName string
}

func NewCreateIndexData(idxName string, tableName string, fieldName string) *CreateIndexData {
	return &CreateIndexData{
		idxName:   idxName,
		tableName: tableName,
		fieldName: fieldName,
	}
}

func (cid *CreateIndexData) IndexName() string {
	return cid.idxName
}

func (cid *CreateIndexData) TableName() string {
	return cid.tableName
}

func (cid *CreateIndexData) FieldName() string {
	return cid.fieldName
}
