package util

import (
	"fmt"
	"os"
	"testing"
)

func TestAppendAndReadEntry(t *testing.T) {
	// Create a temporary file for testing.
	tmpfile, err := os.CreateTemp(".", "wal_test")
	if err != nil {
		fmt.Println("Error creating temporary file:", err)
		t.Fatal(err)
	}
	defer os.Remove(tmpfile.Name())
	defer tmpfile.Close()

	// Create a WAL instance.
	wal, _ := NewWAL(tmpfile.Name())
	if wal == nil {
		t.Fatal("Error creating WAL")
	}
	defer wal.Close()

	// Test data.
	operation := "SET"
	key := []byte("test_key")
	value := []byte("test_value")

	// Append an entry to the WAL.
	err = wal.AppendEntry(WatermarkPlaceholder, operation, key, value)
	if err != nil {
		t.Fatal(err)
	}

	// Read the entry from the WAL.
	readEntry, _, _, err := readWALEntryAt(tmpfile, 0)
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("Operation: %s\n", readEntry.Operation)
	t.Logf("Key Length: %d\n", len(readEntry.Key))
	t.Logf("Key: %s\n", readEntry.Key)
	t.Logf("Value Length: %d\n", len(readEntry.Value))
	t.Logf("Value: %s\n", readEntry.Value)

	// Compare the written and read data.
	if readEntry.Operation != operation || !bytesEqual(readEntry.Key, key) || !bytesEqual(readEntry.Value, value) {
		t.Errorf("Expected %+v, got %+v", WALEntry{Operation: operation, Key: key, Value: value}, readEntry)
	}
}

func TestAppendOnlyPrinciple(t *testing.T) {
	// Create a temporary WAL file for testing.
	tmpfile, err := os.CreateTemp(".", "wal_test")
	if err != nil {
		t.Fatal("Error creating temporary file:", err)
	}
	defer os.Remove(tmpfile.Name())
	defer tmpfile.Close()

	// Create a WAL instance.
	wal, _ := NewWAL(tmpfile.Name())
	if wal == nil {
		t.Fatal("Error creating WAL")
	}
	defer wal.Close()

	// Test data.
	key1 := []byte("key1")
	value1 := []byte("value1")
	key2 := []byte("key2")
	value2 := []byte("value2")

	// Append the first key-value pair to the WAL.
	err = wal.AppendEntry(WatermarkPlaceholder, "SET", key1, value1)
	if err != nil {
		t.Fatal("Error appending entry:", err)
	}

	// Append the second key-value pair to the WAL.
	err = wal.AppendEntry(WatermarkPlaceholder, "SET", key2, value2)
	if err != nil {
		t.Fatal("Error appending entry:", err)
	}

	readEntry1, currentPos, _, err := readWALEntryAt(tmpfile, 0)
	if err != nil {
		t.Fatal("Error reading entry from WAL:", err)
	}

	t.Logf("readEntry1:\n")
	t.Logf("Operation: %s\n", readEntry1.Operation)
	t.Logf("Key Length: %d\n", len(readEntry1.Key))
	t.Logf("Key: %s\n", readEntry1.Key)
	t.Logf("Value Length: %d\n", len(readEntry1.Value))
	t.Logf("Value: %s\n", readEntry1.Value)

	t.Logf("---Current position: %d", currentPos)

	// Read the second entry from the WAL.
	readEntry2, _, _, err := readWALEntryAt(tmpfile, currentPos)
	if err != nil {
		t.Fatal("Error reading entry from WAL:", err)
	}

	t.Logf("readEntry2:\n")
	t.Logf("Operation: %s\n", readEntry2.Operation)
	t.Logf("Key Length: %d\n", len(readEntry2.Key))
	t.Logf("Key: %s\n", readEntry2.Key)
	t.Logf("Value Length: %d\n", len(readEntry2.Value))
	t.Logf("Value: %s\n", readEntry2.Value)

	// Check that the second key-value pair is the last one.
	if !bytesEqual(readEntry2.Key, key2) || !bytesEqual(readEntry2.Value, value2) {
		t.Errorf("Expected %+s, got %+s", WALEntry{Operation: readEntry2.Operation, Key: key2, Value: value2}, readEntry2)
	}
}
