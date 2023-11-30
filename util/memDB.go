package util

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/huandu/skiplist"
)

type MemDB struct {
	skiplist *skiplist.SkipList
	wal      *WAL
}

type Value struct {
	Operation string
	Value     []byte
}

func NewValue(operation string, value []byte) *Value {
	return &Value{
		Operation: operation,
		Value:     value,
	}
}

func NewMemDB() (*MemDB, error) {
	wal, err := NewWAL("disk/walStorage/wal.bin")
	if err != nil {
		return nil, err
	}

	mem := &MemDB{
		skiplist: skiplist.New(skiplist.Bytes),
		wal:      wal,
	}

	// Load the contents from the WAL
	if err := mem.Load(); err != nil {
		return nil, err
	}

	return mem, nil
}

// For testing
func NewMemDBtest() (*MemDB, error) {
	wal, err := NewWAL("../disk/walStorage/wal.bin")
	if err != nil {
		return nil, err
	}

	mem := &MemDB{
		skiplist: skiplist.New(skiplist.Bytes),
		wal:      wal,
	}

	return mem, nil
}

func (mem *MemDB) Set(key []byte, value []byte) error {
	mem.skiplist.Set(key, NewValue("SET", value))

	// Write the operation to the WAL
	err := mem.wal.AppendEntry(WatermarkPlaceholder, "SET", key, value)
	if err != nil {
		return err
	}

	return nil
}

func (mem *MemDB) Get(key []byte) ([]byte, error) {
	elem := mem.skiplist.Get(key)
	if elem.Value.(*Value).Operation == "DEL" {
		return nil, errors.New("Key not found")
	}
	if elem == nil {
		FindValueInSSTFiles(key)
	}
	return elem.Value.(*Value).Value, nil
}

func (mem *MemDB) Del(key []byte) ([]byte, error) {
	elem := mem.skiplist.Get(key)
	if elem == nil || elem.Value.(*Value).Operation == "DEL" {
		return nil, errors.New("Key not found")
	}
	mem.skiplist.Set(key, NewValue("DEL", elem.Value.(*Value).Value))

	// Write the operation to the WAL
	err := mem.wal.AppendEntry(WatermarkPlaceholder, "DEL", key, elem.Value.(*Value).Value)
	if err != nil {
		return nil, err
	}

	return elem.Value.(*Value).Value, nil
}

func (mem *MemDB) FlushToDisk() error {
	// Get the first element in the skiplist
	firstElement := mem.skiplist.Front()

	// If the skiplist is empty, nothing to flush
	if firstElement == nil {
		return nil
	}

	var smallestKey, longestKey []byte

	// Iterate through the skiplist and collect tuples
	var tuples []SSTTuple
	for elem := firstElement; elem != nil; elem = elem.Next() {
		key, ok := elem.Key().([]byte)
		if !ok {
			// Handle the case where the key is not of type []byte
			return errors.New("Key is not of type []byte")
		}

		// Use a type assertion to get the *Value from the interface{}
		valueInterface := elem.Value
		value, ok := valueInterface.(*Value)
		if !ok {
			return errors.New("Value is not of type *Value")
		}

		// Track the smallest key
		if smallestKey == nil || bytes.Compare(key, smallestKey) < 0 {
			smallestKey = key
		}

		// Track the longest key
		if longestKey == nil || bytes.Compare(key, longestKey) > 0 {
			longestKey = key
		}

		tuples = append(tuples, SSTTuple{Key: key, Value: *value})
	}

	// Create a new SST file
	sstFile, err := NewSSTFile()
	if err != nil {
		return err
	}
	defer sstFile.Close()

	// Build the SST file header
	header := SSTFileHeader{
		Magic:       []byte("SSTF"),
		EntryCount:  uint32(len(tuples)),
		SmallestKey: smallestKey,
		LongestKey:  longestKey,
		Version:     uint16(1),
	}

	// Write the header to the SST file
	err = sstFile.writeHeader(header)
	if err != nil {
		return err
	}

	// Write each tuple to the SST file
	for _, tuple := range tuples {
		err := sstFile.writeTuple(tuple.Key, tuple.Value)
		if err != nil {
			return err
		}
	}

	//Update the watermark in WAL
	mem.wal.UpdateWatermark()

	return nil
}

func (mem *MemDB) Load() error {
	// Get the current file size.
	fileInfo, err := mem.wal.file.Stat()
	if err != nil {
		return err
	}
	fileSize := fileInfo.Size()

	// If the file is empty, there is nothing to load.
	if fileSize == 0 {
		return nil
	}

	// Iterate through the entire WAL file.
	for offset := int64(0); offset < fileSize; {
		entry, nextOffset, watermark, err := readWALEntryAt(mem.wal.file, offset)
		if err != nil {
			return err
		}

		// Check if the entry has the watermark placeholder.
		if watermark == WatermarkPlaceholder {
			switch entry.Operation {
			case "SET":
				mem.skiplist.Set(entry.Key, NewValue("SET", entry.Value))
			case "DEL":
				mem.skiplist.Set(entry.Key, NewValue("DEL", entry.Value))
			default:
				return errors.New("Unknown operation in WAL")
			}
		}

		// Break out of the loop if nextOffset is beyond the file size.
		if nextOffset >= fileSize {
			break
		}

		// Move to the next entry.
		offset = nextOffset
	}

	return nil
}

// FindValueInSSTFiles searches through SST files for a given key.
func FindValueInSSTFiles(key []byte) (string, error) {
	// Find the latest SST file number.
	latestFileNumber, err := findLastSSTNumber(filepath.Join("disk/sstStorage"))
	if err != nil {
		return "", err
	}

	// Iterate through the SST files in reverse order.
	for i := latestFileNumber; i > 0; i-- {
		fileName := fmt.Sprintf("sst%03d", i)
		value, x, _ := getValueFromSSTFile(fileName, key)
		if x == 1 {
			return value, nil
		} else if x == 0 {
			return "", fmt.Errorf("key '%s' not found, deleted", key)
		}
		// Continue to the next file if the key wasn't found.
	}

	return "", fmt.Errorf("key '%s' not found in any SST file", key)
}

// getValueFromSSTFile opens an SST file and retrieves a value for a given key.
func getValueFromSSTFile(fileName string, key []byte) (string, int, error) {
	file, err := os.Open(filepath.Join("disk/sstStorage", fileName))
	if err != nil {
		return "", 2, err
	}
	defer file.Close()

	sstFile := &SSTFile{File: file}
	return sstFile.GetValueByKey(key)
}
