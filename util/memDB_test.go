package util

import (
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestMemDBFlushToDisk(t *testing.T) {
	// Create a new MemDB
	mem, err := NewMemDBtest()
	if err != nil {
		t.Fatalf("Error creating MemDB: %v", err)
	}

	// Insert some data into the MemDB
	mem.Set([]byte("apple"), []byte("fruit"))
	mem.Set([]byte("banana"), []byte("yellow"))
	mem.Set([]byte("cherry"), []byte("red"))

	// Define the expected content of the SST file
	expectedContent := append([]byte("SSTF"),
		byte(0), byte(0), byte(0), byte(3), // Entry count
		0, 0, 0, 5, // Smallest key length
		'a', 'p', 'p', 'l', 'e', // Smallest key
		0, 0, 0, 6, // Longest key length
		'c', 'h', 'e', 'r', 'r', 'y', // Longest key
		0, 1, // Version
		'S', 'E', 'T', // Operation
		0, 0, 0, 5, // Tuple 1 key length
		'a', 'p', 'p', 'l', 'e', // Tuple 1 key
		0, 0, 0, 5, // Tuple 1 value length
		'f', 'r', 'u', 'i', 't', // Tuple 1 value
		'S', 'E', 'T', // Operation
		0, 0, 0, 6, // Tuple 2 key length
		'b', 'a', 'n', 'a', 'n', 'a', // Tuple 2 key
		0, 0, 0, 6, // Tuple 2 value length
		'y', 'e', 'l', 'l', 'o', 'w', // Tuple 2 value
		'S', 'E', 'T', // Operation
		0, 0, 0, 6, // Tuple 3 key length
		'c', 'h', 'e', 'r', 'r', 'y', // Tuple 3 key
		0, 0, 0, 3, // Tuple 3 value length
		'r', 'e', 'd', // Tuple 3 value
	)

	// Call the flushToDisk function
	err = mem.FlushToDisk()
	if err != nil {
		t.Fatalf("Error flushing MemDB to disk: %v", err)
	}
	// Get the last SST file number
	lastSSTNumber := findLastSSTNumber(filepath.Join("..", "disk", "sstStorage"))
	if lastSSTNumber <= 0 {
		t.Fatalf("Error finding the last SST file number: %v", err)
	}

	// Open the last SST file
	lastSSTFile := fmt.Sprintf("sst%03d", lastSSTNumber)
	file, err := os.Open(filepath.Join("..", "disk", "sstStorage", lastSSTFile))
	if err != nil {
		t.Fatalf("Error opening SST file: %v", err)
	}
	defer file.Close()

	// Get file size
	fileInfo, err := file.Stat()
	if err != nil {
		t.Fatalf("Error getting file info: %v", err)
	}
	fileSize := fileInfo.Size()

	// Read file content
	fileContent := make([]byte, fileSize)
	_, err = file.Read(fileContent)
	if err != nil {
		t.Fatalf("Error reading SST file: %v", err)
	}

	// Verify file content
	if !reflect.DeepEqual(fileContent, expectedContent) {
		t.Errorf("File content does not match expected content")
	}
}
