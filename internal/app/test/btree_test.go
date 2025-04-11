package test

import (
	"centauri/internal/app/buffer"
	"centauri/internal/app/file"
	"centauri/internal/app/index/btree"
	"centauri/internal/app/log"
	"centauri/internal/app/record"
	"centauri/internal/app/record/schema"
	"centauri/internal/app/tx"
	"centauri/internal/app/types"
	"fmt"
	"os"
	"testing"
)

func createTempDB(t *testing.T) string {
	tempDir, err := os.MkdirTemp("", "btree-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}

	return tempDir
}

// createTx creates a new transaction for testing
func createTx(t *testing.T, dbDir string) *tx.Transaction {
	fm, err := file.NewFileManager(dbDir, 400)
	if err != nil {
		t.Fatalf("Failed to create file manager: %v", err)
	}

	lm, err := log.NewLogManager(fm, "testlog")
	if err != nil {
		t.Fatalf("Failed to create log manager: %v", err)
	}

	numBuff := 3
	bm := buffer.NewBufferManager(fm, lm, numBuff)

	// Initialize a transaction with appropriate settings for testing
	tx := tx.NewTransaction(fm, lm, bm)

	return tx
}

// Creates a new B-tree index with int keys
func createIntIndex(t *testing.T, tx *tx.Transaction, idxname string) *btree.BTreeIndex {
	// Create schema for leaf pages with integer keys
	sch := schema.NewSchema()
	sch.AddIntField("dataval")
	sch.AddIntField("block")
	sch.AddIntField("id")

	// Create layout for leaf pages
	layout := record.NewLayout(sch)

	return btree.NewBTreeIndex(tx, idxname, layout)
}

// Creates a new B-tree index with string keys
func createStringIndex(t *testing.T, tx *tx.Transaction, idxname string) *btree.BTreeIndex {
	// Create schema for leaf pages with integer keys
	sch := schema.NewSchema()
	sch.AddStringField("dataval", 20)
	sch.AddStringField("block", 20)
	sch.AddStringField("id", 20)

	// Create layout for leaf pages
	layout := record.NewLayout(sch)

	return btree.NewBTreeIndex(tx, idxname, layout)
}

// Verifies basic operations on an empty index
func TestEmptyIndex(t *testing.T) {
	dbDir := createTempDB(t)
	defer os.RemoveAll(dbDir)

	txn := createTx(t, dbDir)
	defer txn.Commit()

	idx := createIntIndex(t, txn, "emptytest")
	defer idx.Close()

	// Test search on empty index
	searchKey := types.NewConstantInt(42)
	idx.BeforeFirst(searchKey)

	// Next should return false since there are no matches
	if idx.Next() {
		t.Errorf("Expected Next() to return false on empty index")
	}
}

func TestBasicInsertAndSearch(t *testing.T) {
	dbDir := createTempDB(t)
	defer os.RemoveAll(dbDir)

	txn := createTx(t, dbDir)
	defer txn.Commit()

	idx := createIntIndex(t, txn, "emptytest")
	defer idx.Close()

	// Insert a single record
	key := types.NewConstantInt(42)
	rid := types.NewRID(1, 1) // Block 1, slot 1
	idx.Insert(key, rid)

	// Position the index before trying to search
	idx.BeforeFirst(key)

	// Should find the record
	if !idx.Next() {
		t.Fatalf("Failed to find inserted record with key 42")
	}

	// Verify the RID is correct
	foundRid := idx.GetDataRid()

	// Verify RID is correct
	if !foundRid.Equals(rid) {
		t.Errorf("Retrieved incorrect RID: got %v, want %v", foundRid, rid)
	}

	// No more records with this key
	if idx.Next() {
		t.Errorf("Found unexpected addtional record with key 42")
	}
}

func TestMultipleInserts(t *testing.T) {
	dbDir := createTempDB(t)
	defer os.RemoveAll(dbDir)

	txn := createTx(t, dbDir)
	defer txn.Commit()

	idx := createIntIndex(t, txn, "multitest")
	defer idx.Close()

	// Insert multiple records with different keys
	keys := []int{50, 10, 90, 30, 70, 20, 80, 40, 60}
	for i, keyVal := range keys {
		key := types.NewConstantInt(keyVal)
		rid := types.NewRID(i+1, i+1) // Block i+1, slot i+1
		idx.Insert(key, rid)
	}

	// Search for each key and verify the RID
	for i, keyVal := range keys {
		key := types.NewConstantInt(keyVal)
		expectedRid := types.NewRID(i+1, i+1)

		idx.BeforeFirst(key)
		if !idx.Next() {
			t.Errorf("Failed to find inserted record with key %d", keyVal)
			continue
		}

		foundRid := idx.GetDataRid()
		if !foundRid.Equals(expectedRid) {
			t.Errorf("Key %d: Retrieved incorrect RID: got %v, want %v",
				keyVal, foundRid, expectedRid)
		}

		// No more records with this key
		if idx.Next() {
			t.Errorf("Found unexpected additional record with key %d", keyVal)
		}
	}
}

// TestDuplicateKeys tests inserting and retrieving multiple records with the same key
func TestDuplicateKeys(t *testing.T) {
	dbDir := createTempDB(t)
	defer os.RemoveAll(dbDir)

	txn := createTx(t, dbDir)
	defer txn.Commit()

	idx := createIntIndex(t, txn, "duptest")
	defer idx.Close()

	// Insert multiple records with the same key
	key := types.NewConstantInt(42)
	numRecords := 10
	expectedRids := make([]*types.RID, numRecords)

	for i := 0; i < numRecords; i++ {
		rid := types.NewRID(i+1, i+1) // Block i+1, slot i+1
		expectedRids[i] = rid
		idx.Insert(key, rid)
	}

	// Search for the key and verify we find all the RIDs
	idx.BeforeFirst(key)

	foundCount := 0
	for idx.Next() {
		if foundCount >= numRecords {
			t.Errorf("Found more records than expected with key 42")
			break
		}

		foundRid := idx.GetDataRid()
		found := false
		for _, expectedRid := range expectedRids {
			if foundRid.Equals(expectedRid) {
				found = true
				break
			}
		}

		if !found {
			t.Errorf("Found unexpected RID %v for key 42", foundRid)
		}

		foundCount++
	}

	if foundCount != numRecords {
		t.Errorf("Expected to find %d records with key 42, but found %d",
			numRecords, foundCount)
	}
}

// TestStringKeys tests the B-tree with string keys
func TestStringKeys(t *testing.T) {
	dbDir := createTempDB(t)
	defer os.RemoveAll(dbDir)

	txn := createTx(t, dbDir)
	defer txn.Commit()

	idx := createStringIndex(t, txn, "stringtest")
	defer idx.Close()

	// Insert records with string keys
	keys := []string{"apple", "banana", "cherry", "date", "elderberry"}
	for i, keyVal := range keys {
		key := types.NewConstantString(keyVal)
		rid := types.NewRID(i+1, i+1) // Block i+1, slot i+1
		idx.Insert(key, rid)
	}

	// Search for each key and verify the RID
	for i, keyVal := range keys {
		key := types.NewConstantString(keyVal)
		expectedRid := types.NewRID(i+1, i+1)

		idx.BeforeFirst(key)
		if !idx.Next() {
			t.Errorf("Failed to find inserted record with key '%s'", keyVal)
			continue
		}

		foundRid := idx.GetDataRid()
		if !foundRid.Equals(expectedRid) {
			t.Errorf("Key '%s': Retrieved incorrect RID: got %v, want %v",
				keyVal, foundRid, expectedRid)
		}
	}
}

// TestDeleteRecords tests deleting records from the B-tree
func TestDeleteRecords(t *testing.T) {
	dbDir := createTempDB(t)
	defer os.RemoveAll(dbDir)

	txn := createTx(t, dbDir)
	defer txn.Commit()

	idx := createIntIndex(t, txn, "deletetest")
	defer idx.Close()

	// Insert records
	key1 := types.NewConstantInt(10)
	key2 := types.NewConstantInt(20)
	rid1 := types.NewRID(1, 1)
	rid2 := types.NewRID(2, 2)

	idx.Insert(key1, rid1)
	idx.Insert(key2, rid2)

	// Verify both records exist
	idx.BeforeFirst(key1)
	if !idx.Next() {
		t.Fatalf("Failed to find record with key 10 before deletion")
	}

	idx.BeforeFirst(key2)
	if !idx.Next() {
		t.Fatalf("Failed to find record with key 20 before deletion")
	}

	// Delete the first record
	idx.Delete(key1, rid1)

	// Verify it's gone
	idx.BeforeFirst(key1)
	if idx.Next() {
		t.Errorf("Record with key 10 still exists after deletion")
	}

	// Second record should still be there
	idx.BeforeFirst(key2)
	if !idx.Next() {
		t.Errorf("Record with key 20 missing after deletion of different key")
	}
}

// TestManyRecords tests inserting many records to force page splits
func TestManyRecords(t *testing.T) {
	dbDir := createTempDB(t)
	defer os.RemoveAll(dbDir)

	txn := createTx(t, dbDir)
	defer txn.Commit()

	idx := createIntIndex(t, txn, "manytest")
	defer idx.Close()

	// Insert many records to force page splits
	numRecords := 200
	for i := 0; i < numRecords; i++ {
		key := types.NewConstantInt(i)
		rid := types.NewRID(i/10+1, i%10+1) // Distribute across blocks
		idx.Insert(key, rid)
	}

	// Verify we can find each record
	for i := 0; i < numRecords; i++ {
		key := types.NewConstantInt(i)
		expectedRid := types.NewRID(i/10+1, i%10+1)

		idx.BeforeFirst(key)
		if !idx.Next() {
			t.Errorf("Failed to find inserted record with key %d", i)
			continue
		}

		foundRid := idx.GetDataRid()
		if !foundRid.Equals(expectedRid) {
			t.Errorf("Key %d: Retrieved incorrect RID: got %v, want %v",
				i, foundRid, expectedRid)
		}
	}
}

// TestManyDuplicateKeys tests inserting many records with the same key to test overflow blocks
func TestManyDuplicateKeys(t *testing.T) {
	dbDir := createTempDB(t)
	defer os.RemoveAll(dbDir)

	txn := createTx(t, dbDir)
	defer txn.Commit()

	idx := createIntIndex(t, txn, "overflowtest")
	defer idx.Close()

	// Insert many records with the same key to test overflow blocks
	key := types.NewConstantInt(42)
	numRecords := 100 // Should force multiple overflow blocks
	expectedRids := make([]*types.RID, numRecords)

	for i := 0; i < numRecords; i++ {
		rid := types.NewRID(i/10+1, i%10+1) // Distribute across blocks
		expectedRids[i] = rid
		idx.Insert(key, rid)
	}

	// Search for the key and verify we find all the RIDs
	idx.BeforeFirst(key)

	foundCount := 0
	for idx.Next() {
		if foundCount >= numRecords {
			t.Errorf("Found more records than expected with key 42")
			break
		}

		foundRid := idx.GetDataRid()
		found := false
		for _, expectedRid := range expectedRids {
			if foundRid.Equals(expectedRid) {
				found = true
				break
			}
		}

		if !found {
			t.Errorf("Found unexpected RID %v for key 42", foundRid)
		}

		foundCount++
	}

	if foundCount != numRecords {
		t.Errorf("Expected to find %d records with key 42, but found %d",
			numRecords, foundCount)
	}
}

// TestMixedOperations tests a mix of insert, search, and delete operations
func TestMixedOperations(t *testing.T) {
	dbDir := createTempDB(t)
	defer os.RemoveAll(dbDir)

	txn := createTx(t, dbDir)
	defer txn.Commit()

	idx := createIntIndex(t, txn, "mixedtest")
	defer idx.Close()

	// Insert some initial records
	for i := 0; i < 50; i++ {
		key := types.NewConstantInt(i)
		rid := types.NewRID(i+1, 1)
		idx.Insert(key, rid)
	}

	// Delete some records
	for i := 0; i < 50; i += 2 {
		key := types.NewConstantInt(i)
		rid := types.NewRID(i+1, 1)
		idx.Delete(key, rid)
	}

	// Insert more records, including some with duplicate keys
	for i := 30; i < 80; i++ {
		key := types.NewConstantInt(i)
		rid := types.NewRID(i+1, 2)
		idx.Insert(key, rid)
	}

	// Verify the state
	// 1. Deleted even numbers 0-48 should be gone
	for i := 0; i < 50; i += 2 {
		key := types.NewConstantInt(i)
		idx.BeforeFirst(key)

		if idx.Next() {
			rid := idx.GetDataRid()
			if rid.Slot() == 1 {
				t.Errorf("Record with key %d slot 1 still exists after deletion", i)
			}
		}
	}

	// 2. Odd numbers 1-49 should have exactly one record each
	for i := 1; i < 50; i += 2 {
		key := types.NewConstantInt(i)
		idx.BeforeFirst(key)

		if !idx.Next() {
			t.Errorf("Original record with key %d is missing", i)
		} else {
			rid := idx.GetDataRid()
			if rid.BlockNumber() != i+1 || rid.Slot() != 1 {
				t.Errorf("Record with key %d has incorrect RID: %v", i, rid)
			}

			if idx.Next() {
				t.Errorf("Found unexpected second record with key %d", i)
			}
		}
	}

	// 3. Numbers 30-79 should have the newly inserted records with slot=2
	for i := 30; i < 80; i++ {
		foundSlot2 := false

		key := types.NewConstantInt(i)
		idx.BeforeFirst(key)

		for idx.Next() {
			rid := idx.GetDataRid()
			if rid.BlockNumber() == i+1 && rid.Slot() == 2 {
				foundSlot2 = true
				break
			}
		}

		if !foundSlot2 {
			t.Errorf("Newly inserted record with key %d slot 2 is missing", i)
		}
	}
}

// TestReopenIndex tests creating an index, adding data, closing it, then reopening and verifying contents
func TestReopenIndex(t *testing.T) {
	dbDir := createTempDB(t)
	defer os.RemoveAll(dbDir)

	// First transaction - create and populate the index
	txn1 := createTx(t, dbDir)
	idx1 := createIntIndex(t, txn1, "reopentest")

	// Insert some records
	for i := 0; i < 20; i++ {
		key := types.NewConstantInt(i)
		rid := types.NewRID(i+1, i+1)
		idx1.Insert(key, rid)
	}

	// Close the index and commit the transaction
	idx1.Close()
	txn1.Commit()

	// Second transaction - reopen the index and verify contents
	txn2 := createTx(t, dbDir)
	idx2 := createIntIndex(t, txn2, "reopentest")
	defer idx2.Close()
	defer txn2.Commit()

	// Verify all records are present
	for i := 0; i < 20; i++ {
		key := types.NewConstantInt(i)
		expectedRid := types.NewRID(i+1, i+1)

		idx2.BeforeFirst(key)
		if !idx2.Next() {
			t.Errorf("Failed to find record with key %d after reopening index", i)
			continue
		}

		foundRid := idx2.GetDataRid()
		if !foundRid.Equals(expectedRid) {
			t.Errorf("Record with key %d has incorrect RID after reopening: got %v, want %v",
				i, foundRid, expectedRid)
		}
	}
}

// TestSearchCost tests the SearchCost function
func TestSearchCost(t *testing.T) {
	testCases := []struct {
		numBlocks int
		rpb       int
		expected  int
	}{
		{1, 10, 1},    // Just one block, cost is 1
		{10, 10, 2},   // log_10(10) = 1, so cost is 1+1=2
		{100, 10, 3},  // log_10(100) = 2, so cost is 1+2=3
		{1000, 10, 4}, // log_10(1000) = 3, so cost is 1+3=4
		{100, 100, 2}, // log_100(100) = 1, so cost is 1+1=2
	}

	for i, tc := range testCases {
		t.Run(fmt.Sprintf("Case%d", i), func(t *testing.T) {
			result := btree.SearchCost(tc.numBlocks, tc.rpb)
			if result != tc.expected {
				t.Errorf("SearchCost(%d, %d) = %d, want %d",
					tc.numBlocks, tc.rpb, result, tc.expected)
			}
		})
	}
}
