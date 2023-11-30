package util

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

// TestSSTFileReadWrite tests reading and writing an SST file.
func TestSSTFileReadWrite(t *testing.T) {
	// Create a new SST file
	sstFile, err := NewSSTFile()
	if err != nil {
		t.Fatalf("Error creating SST file: %v", err)
	}

	// Cleanup function to remove the SST file
	t.Cleanup(func() {
		if err := sstFile.Close(); err != nil {
			t.Errorf("Error closing SST file: %v", err)
		}
		if err := os.Remove(sstFile.file.Name()); err != nil {
			t.Errorf("Error removing SST file: %v", err)
		}
	})

	// Define test data
	header := SSTFileHeader{
		Magic:       []byte("SSTF"),
		EntryCount:  uint32(2),
		SmallestKey: []byte("apple"),
		LongestKey:  []byte("orange"),
		Version:     uint16(1),
	}

	tuple1 := SSTTuple{
		Key:   []byte("apple"),
		Value: Value{Operation: "SET", Value: []byte("fruit")},
	}

	tuple2 := SSTTuple{
		Key:   []byte("banana"),
		Value: Value{Operation: "SET", Value: []byte("yellow")},
	}

	// Write the header
	if err := sstFile.writeHeader(header); err != nil {
		t.Fatalf("Error writing SST file header: %v", err)
	}

	// Write tuples
	if err := sstFile.writeTuple(tuple1.Key, tuple1.Value); err != nil {
		t.Fatalf("Error writing tuple 1: %v", err)
	}

	if err := sstFile.writeTuple(tuple2.Key, tuple2.Value); err != nil {
		t.Fatalf("Error writing tuple 2: %v", err)
	}

	// Read the SST file
	file, err := os.Open(sstFile.file.Name())
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
	expectedContent := append(header.Magic,
		byte(0), byte(0), byte(0), byte(2), // Entry count
		0, 0, 0, 5, // Smallest key length
		'a', 'p', 'p', 'l', 'e', // Smallest key
		0, 0, 0, 6, // Longest key length
		'o', 'r', 'a', 'n', 'g', 'e', // Longest key
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
	)

	if !reflect.DeepEqual(fileContent, expectedContent) {
		t.Errorf("File content does not match expected content")
	}
}

// TestSSTFileNumbering tests SST file numbering.
func TestSSTFileNumbering(t *testing.T) {
	// Create three SST files
	sstFile1, err := NewSSTFile()
	if err != nil {
		t.Fatalf("Error creating SST file 1: %v", err)
	}

	sstFile2, err := NewSSTFile()
	if err != nil {
		t.Fatalf("Error creating SST file 2: %v", err)
	}

	sstFile3, err := NewSSTFile()
	if err != nil {
		t.Fatalf("Error creating SST file 3: %v", err)
	}

	// Cleanup function to remove the SST files
	t.Cleanup(func() {
		if err := sstFile1.Close(); err != nil {
			t.Errorf("Error closing SST file 1: %v", err)
		}
		if err := os.Remove(sstFile1.file.Name()); err != nil {
			t.Errorf("Error removing SST file 1: %v", err)
		}

		if err := sstFile2.Close(); err != nil {
			t.Errorf("Error closing SST file 2: %v", err)
		}
		if err := os.Remove(sstFile2.file.Name()); err != nil {
			t.Errorf("Error removing SST file 2: %v", err)
		}

		if err := sstFile3.Close(); err != nil {
			t.Errorf("Error closing SST file 3: %v", err)
		}
		if err := os.Remove(sstFile3.file.Name()); err != nil {
			t.Errorf("Error removing SST file 3: %v", err)
		}
	})

	// Check if the file names are as expected
	expectedFile1 := filepath.Join("..", "disk", "sstStorage", "sst001")
	expectedFile2 := filepath.Join("..", "disk", "sstStorage", "sst002")
	expectedFile3 := filepath.Join("..", "disk", "sstStorage", "sst003")

	t.Logf("Actual File 1: %s\n", sstFile1.file.Name())
	t.Logf("Actual File 2: %s\n", sstFile2.file.Name())
	t.Logf("Actual File 3: %s\n", sstFile3.file.Name())

	t.Logf("Expected File 1: %s\n", expectedFile1)
	t.Logf("Expected File 2: %s\n", expectedFile2)
	t.Logf("Expected File 3: %s\n", expectedFile3)

	if sstFile1.file.Name() != expectedFile1 {
		t.Errorf("File name for SST file 1 does not match expected: got %s, expected %s", sstFile1.file.Name(), expectedFile1)
	}

	if sstFile2.file.Name() != expectedFile2 {
		t.Errorf("File name for SST file 2 does not match expected: got %s, expected %s", sstFile2.file.Name(), expectedFile2)
	}

	if sstFile3.file.Name() != expectedFile3 {
		t.Errorf("File name for SST file 3 does not match expected: got %s, expected %s", sstFile3.file.Name(), expectedFile3)
	}
}
