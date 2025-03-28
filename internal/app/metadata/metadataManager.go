package metadata

import (
	"centauri/internal/app/record"
	"centauri/internal/app/record/schema"
	"centauri/internal/app/tx"
)

// MetaDataManager manages database metadata including tables, views, statistics and indexes.
// It coordinates between different managers to handle all metadata operations.
// It provides a unified interface for metadata management across the database system.
type MetaDataManager struct {
	tm *TableManager
	vm *ViewManager
	sm *StatManager
	im *IndexManager
}

func NewMetaDataManager(isNew bool, tx *tx.Transaction) *MetaDataManager {
	tm := NewTableManager(isNew, tx)
	vm := NewViewManager(isNew, tm, tx)
	sm := NewStatManager(tm, tx)
	im := NewIndexManager(isNew, tm, sm, tx)

	return &MetaDataManager{
		tm: tm,
		vm: vm,
		sm: sm,
		im: im,
	}
}

func (mm *MetaDataManager) CreateTable(tableName string, schema *schema.Schema, tx *tx.Transaction) {
	mm.tm.CreateTable(tableName, schema, tx)
}

func (mm *MetaDataManager) GetLayout(tableName string, tx *tx.Transaction) *record.Layout {
	return mm.tm.GetLayout(tableName, tx)
}

func (mm *MetaDataManager) CreateView(viewName string, viewDef string, tx *tx.Transaction) {
	mm.vm.CreateView(viewName, viewDef, tx)
}

func (mm *MetaDataManager) GetViewDef(viewName string, tx *tx.Transaction) string {
	return mm.vm.GetViewDef(viewName, tx)
}

func (mm *MetaDataManager) CreateIndex(idxName string, tableName string, fieldName string, tx *tx.Transaction) {
	mm.im.CreateIndex(idxName, tableName, fieldName, tx)
}

func (mm *MetaDataManager) GetIndexInfo(tableName string, tx *tx.Transaction) map[string]IndexInfo {
	return mm.im.GetIndexInfo(tableName, tx)
}

func (mm *MetaDataManager) GetStatInfo(tableName string, layout *record.Layout, tx *tx.Transaction) StatInfo {
	return mm.sm.GetStatInfo(tableName, layout, tx)
}
