package server

import (
	"centauri/internal/app/buffer"
	"centauri/internal/app/file"
	"centauri/internal/app/log"
	"centauri/internal/app/metadata"
	"centauri/internal/app/plan"
	"centauri/internal/app/tx"
	"fmt"
	"os"
	"sync"
)

const BLOCK_SIZE = 400
const BUFFER_SIZE = 8
const LOG_FILE = "centauridb.log"

type CentauriDB struct {
	fm      *file.FileManager
	bm      *buffer.BufferManager
	lm      *log.LogManager
	mdm     *metadata.MetaDataManager
	planner *plan.Planner
	mu      sync.RWMutex
}

// Creates a new CentauriDb instance with custom configuration
func NewCentauriDBWithConfig(dirName string, blockSize int, buffSize int) (*CentauriDB, error) {
	if err := os.MkdirAll(dirName, 0755); err != nil {
		return nil, fmt.Errorf("failed to create dirctory: %w", err)
	}

	db := &CentauriDB{}

	// Intialize the File Manager
	fm, err := file.NewFileManager(dirName, blockSize)
	if err != nil {
		return nil, fmt.Errorf("failed to create file manager: %w", err)
	}
	db.fm = fm

	// Intialize the Log Manager
	lm, err := log.NewLogManager(fm, LOG_FILE)
	if err != nil {
		return nil, fmt.Errorf("failed to create log manager: %w", err)
	}
	db.lm = lm

	// Intialize the Buffer Manager
	bm := buffer.NewBufferManager(fm, lm, buffSize)
	db.bm = bm

	return db, nil
}

// Creates a new CentauriDB instance with default configuration
// and initializes the metadata table
func NewCentauriDB(dirName string) (*CentauriDB, error) {
	db, err := NewCentauriDBWithConfig(dirName, BLOCK_SIZE, BUFFER_SIZE)

	if err != nil {
		return nil, err
	}

	tx := db.NewTx()

	// Check if this is a new database
	isNew := db.fm.IsNew()

	if isNew {
		fmt.Println("creating new database")
	} else {
		fmt.Println("recovering existing database")
		if err := tx.Recover(); err != nil {
			return nil, fmt.Errorf("recovery failed: %w", &err)
		}
	}

	// Initialize metadata manager
	mdm := metadata.NewMetaDataManager(isNew, tx)
	db.mdm = mdm

	// Initialize query and update planners
	qp := plan.NewBasicQueryPlanner(mdm)
	up := plan.NewBasicUpdatePlanner(mdm)

	db.planner = plan.NewPlanner(qp, up)

	// Commit the transaction
	tx.Commit()

	// if err := tx.Commit(); err != nil {
	// 	return nil, fmt.Errorf("failed to commit transaction: %w", &err)
	// }

	return db, nil
}

func (db *CentauriDB) NewTx() *tx.Transaction {
	return tx.NewTransaction(db.fm, db.lm, db.bm)
}

func (db *CentauriDB) MdMgr() *metadata.MetaDataManager {
	return db.mdm
}

func (db *CentauriDB) Planner() *plan.Planner {
	return db.planner
}

func (db *CentauriDB) FileMgr() *file.FileManager {
	return db.fm
}

func (db *CentauriDB) LogMgr() *log.LogManager {
	return db.lm
}

func (db *CentauriDB) BufferMgr() *buffer.BufferManager {
	return db.bm
}
