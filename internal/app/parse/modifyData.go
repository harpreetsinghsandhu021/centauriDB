package parse

import (
	"centauri/internal/app/query"
)

type ModifyData struct {
	tableName string
	fieldName string
	newVal    *query.Expression
	pred      *query.Predicate
}

func NewModifyData(tableName string, fieldName string, newVal *query.Expression, pred *query.Predicate) *ModifyData {
	return &ModifyData{
		tableName: tableName,
		fieldName: fieldName,
		newVal:    newVal,
		pred:      pred,
	}
}

func (md *ModifyData) TableName() string {
	return md.tableName
}

func (md *ModifyData) TargetField() string {
	return md.fieldName
}

func (md *ModifyData) NewValue() *query.Expression {
	return md.newVal
}

func (md *ModifyData) Pred() *query.Predicate {
	return md.pred
}
