package test

import (
	"centauri/internal/app/file"
	"testing"
)

func TestNewBlockID(t *testing.T) {
	tests := []struct {
		name        string
		filename    string
		blockNumber int
	}{
		{"Basic creation", "test.txt", 1},
		{"Empty filename", "", 0},
		{"Negative block number", "file.dat", -1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			blockId := file.NewBlockID(tt.filename, tt.blockNumber)

			if blockId.FileName() != tt.filename {
				t.Errorf("NewBlockId().filename = %v, want %v", blockId.FileName(), tt.filename)
			}

			if blockId.Number() != tt.blockNumber {
				t.Errorf("NewBlockID().blockNumber = %v, want %v", blockId.Number(), tt.blockNumber)
			}
		})
	}
}

func TestBlockID_FileName(t *testing.T) {
	blockId := file.NewBlockID("test.txt", 1)
	if got := blockId.FileName(); got != "test.txt" {
		t.Errorf("BlockID.FileName() = %v, want %v", got, "test.txt")
	}
}

func TestBlockID_Number(t *testing.T) {
	blockId := file.NewBlockID("test.txt", 1)
	if got := blockId.Number(); got != 1 {
		t.Errorf("BlockID.Number() = %v, want %v", got, 1)
	}
}

func TestBlockId_Equals(t *testing.T) {
	tests := []struct {
		name     string
		blockId1 *file.BlockID
		blockId2 *file.BlockID
		want     bool
	}{
		{
			name:     "Equal BlockIds",
			blockId1: file.NewBlockID("test.txt", 1),
			blockId2: file.NewBlockID("test.txt", 1),
			want:     true,
		},
		{
			name:     "Different Filenames",
			blockId1: file.NewBlockID("test1.txt", 1),
			blockId2: file.NewBlockID("test2.txt", 1),
			want:     false,
		},
		{
			name:     "Different block numbers",
			blockId1: file.NewBlockID("test.txt", 1),
			blockId2: file.NewBlockID("test.txt", 2),
			want:     false,
		},
		{
			name:     "Nil comparison",
			blockId1: file.NewBlockID("test.txt", 1),
			blockId2: nil,
			want:     false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.blockId1.Equals(tt.blockId2); got != tt.want {
				t.Errorf("BlockId.Equals() = %v, want %v", got, tt.want)
			}
		})
	}

}

func TestBlockID_ToString(t *testing.T) {
	tests := []struct {
		name     string
		blockID  *file.BlockID
		expected string
	}{
		{
			name:     "Basic toString",
			blockID:  file.NewBlockID("test.txt", 1),
			expected: "[file test.txt, block 1]",
		},
		{
			name:     "Empty filename",
			blockID:  file.NewBlockID("", 0),
			expected: "[file , block 0]",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.blockID.String(); got != tt.expected {
				t.Errorf("BlockID.toString() = %v, want %v", got, tt.expected)
			}
		})
	}
}

func TestBlockID_HashCode(t *testing.T) {
	tests := []struct {
		name     string
		blockID1 *file.BlockID
		blockID2 *file.BlockID
		wantSame bool
	}{
		{
			name:     "Same BlockIDs should have same hash",
			blockID1: file.NewBlockID("test.txt", 1),
			blockID2: file.NewBlockID("test.txt", 1),
			wantSame: true,
		},
		{
			name:     "Different BlockIDs should have different hash",
			blockID1: file.NewBlockID("test1.txt", 1),
			blockID2: file.NewBlockID("test2.txt", 1),
			wantSame: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			hash1 := tt.blockID1.HashCode()
			hash2 := tt.blockID2.HashCode()
			if (hash1 == hash2) != tt.wantSame {
				t.Errorf("BlockID.hashCode() equality = %v, want %v", hash1 == hash2, tt.wantSame)
			}
		})
	}
}

func TestBlockID_HashCode_Consistency(t *testing.T) {
	blockID := file.NewBlockID("test.txt", 1)
	hash1 := blockID.HashCode()
	hash2 := blockID.HashCode()
	if hash1 != hash2 {
		t.Errorf("BlockID.hashCode() not consistent: got %v and %v for same BlockID", hash1, hash2)
	}
}
