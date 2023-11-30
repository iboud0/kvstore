package util

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"os"
)

const (
	Watermark            uint32 = 0xDEAD
	WatermarkPlaceholder uint32 = 0
)

// WALEntry represents an entry in the Write-Ahead Log.
type WALEntry struct {
	Operation string
	Key       []byte
	Value     []byte
}

// WAL represents the Write-Ahead Log.
type WAL struct {
	file *os.File
}

func NewWAL(filename string) (*WAL, error) {
	file, err := os.OpenFile(filename, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, fmt.Errorf("error opening/creating WAL file: %v", err)
	}

	return &WAL{file: file}, nil
}

// AppendEntry appends a new entry to the Write-Ahead Log.
func (w *WAL) AppendEntry(watermark uint32, operation string, key, value []byte) error {
	entry := WALEntry{
		Operation: operation, // Operations are either SET or DEL.
		Key:       key,
		Value:     value,
	}

	// Write the placeholder for the watermark as the first 4 bytes.
	if err := binary.Write(w.file, binary.BigEndian, watermark); err != nil {
		return err
	}

	// Write the operation type to the WAL.
	// w.file.WriteString(entry.Operation)
	if err := binary.Write(w.file, binary.BigEndian, []byte(entry.Operation)); err != nil {
		return err
	}

	// Write the key length and key to the WAL.
	// Convert the key length to a 4-byte slice in little-endian order before writing it.
	if err := binary.Write(w.file, binary.BigEndian, uint32(len(entry.Key))); err != nil {
		return err
	}
	// Write the key.
	// w.file.Write(entry.Key)
	if err := binary.Write(w.file, binary.BigEndian, entry.Key); err != nil {
		return err
	}

	// Write the value length and value to the WAL.
	// Convert the value length to a 4-byte slice in little-endian order before writing it.
	if err := binary.Write(w.file, binary.BigEndian, uint32(len(entry.Value))); err != nil {
		return err
	}
	// Write the key.
	// w.file.Write(entry.Value)
	if err := binary.Write(w.file, binary.BigEndian, entry.Value); err != nil {
		return err
	}

	return nil
}

// Close closes the Write-Ahead Log.
func (w *WAL) Close() error {
	return w.file.Close()
}

func readWALEntryAt(file *os.File, offset int64) (WALEntry, int64, uint32, error) {
	var entry WALEntry

	// Seek to the specified offset in the file.
	_, err := file.Seek(offset, io.SeekStart)
	if err != nil {
		return entry, 0, 1, err
	}

	// Use bufio.Reader to read the file.
	reader := bufio.NewReader(file)

	// Read the watermark value from the WAL.
	var watermark_ uint32
	if err := binary.Read(reader, binary.BigEndian, &watermark_); err != nil {
		return entry, 0, 1, err
	}

	// Check if the watermark value is valid.
	if watermark_ != WatermarkPlaceholder && watermark_ != Watermark {
		return entry, 0, 1, fmt.Errorf("Invalid watermark value")
	}

	// Read the operation type from the WAL.
	opBuf := make([]byte, 3) // Assuming the maximum length of the operation is 3 characters.
	if _, err := io.ReadFull(reader, opBuf); err != nil {
		return entry, 0, 1, err
	}
	entry.Operation = string(opBuf)

	// Read the key length from the WAL.
	var keyLen uint32
	if err := binary.Read(reader, binary.BigEndian, &keyLen); err != nil {
		return entry, 0, 1, err
	}

	// Read the key from the WAL.
	keyBuf := make([]byte, keyLen)
	n, err := io.ReadFull(reader, keyBuf)
	if err != nil {
		return entry, 0, 1, err
	}
	if n != int(keyLen) {
		return entry, 0, 1, fmt.Errorf("unexpected number of bytes read for key: expected %d, got %d", keyLen, n)
	}
	entry.Key = keyBuf

	// Read the value length from the WAL.
	var valLen uint32
	if err := binary.Read(reader, binary.BigEndian, &valLen); err != nil {
		return entry, 0, 1, err
	}

	// Read the value from the WAL.
	valBuf := make([]byte, valLen)
	if n, err := io.ReadFull(reader, valBuf); err != nil {
		return entry, 0, 1, err
	} else if n != int(valLen) {
		return entry, 0, 1, fmt.Errorf("unexpected number of bytes read for value: expected %d, got %d", valLen, n)
	}
	entry.Value = valBuf

	// Get the current position in the file after reading the entry.
	currentPos := int64(3+keyLen+valLen+4*2+4) + offset

	return entry, currentPos, watermark_, nil
}

// Helper function to compare two byte slices.
func bytesEqual(a, b []byte) bool {
	return string(a) == string(b)
}

// LastOperation returns the last operation from the WAL.
func (w *WAL) LastOperation() (*WALEntry, error) {
	// Get the current file size.
	fileInfo, err := w.file.Stat()
	if err != nil {
		return nil, err
	}
	fileSize := fileInfo.Size()

	// If the file is empty, there is no last operation.
	if fileSize == 0 {
		return nil, nil
	}

	var lastEntry *WALEntry

	// Iterate through the entire WAL file.
	for offset := int64(0); offset < fileSize; {
		entry, nextOffset, _, err := readWALEntryAt(w.file, offset)
		if err != nil {
			fmt.Println("Error reading entry:", err)
			return nil, err
		}

		// Update the last entry.
		lastEntry = &entry

		// Move to the next entry.
		offset = nextOffset
	}

	return lastEntry, nil
}

// RewriteAndReplaceWatermark rewrites all operations in the WAL, modifying only the watermark placeholder.
// It also replaces the watermark placeholder in the last operation with the actual watermark.
func (w *WAL) UpdateWatermark() error {
	// Create a new WAL to store the modified content.
	newWAL, err := NewWAL("disk/walStorage/new_wal.bin")
	if err != nil {
		return err
	}
	defer newWAL.Close()

	// Get the current file size.
	fileInfo, err := w.file.Stat()
	if err != nil {
		return err
	}
	fileSize := fileInfo.Size()

	// If the file is empty, nothing to rewrite.
	if fileSize == 0 {
		return nil
	}

	// Iterate through the entire WAL file.
	for offset := int64(0); offset < fileSize; {
		entry, nextOffset, _, err := readWALEntryAt(w.file, offset)
		if err != nil {
			return err
		}

		// Write the modified entry to the new WAL.
		if nextOffset == fileSize {
			// This is the last entry, replace watermark placeholder with actual watermark.
			if err := newWAL.AppendEntry(Watermark, entry.Operation, entry.Key, entry.Value); err != nil {
				return err
			}
		} else {
			// Not the last entry, use watermark placeholder.
			if err := newWAL.AppendEntry(Watermark, entry.Operation, entry.Key, entry.Value); err != nil {
				return err
			}
		}

		// Move to the next entry.
		offset = nextOffset
	}

	// Close both the original and new WAL files.
	if err := w.Close(); err != nil {
		return err
	}
	if err := newWAL.Close(); err != nil {
		return err
	}

	// Replace the original WAL with the new one.
	if err := os.Rename("disk/walStorage/new_wal.bin", "disk/walStorage/wal.bin"); err != nil {
		return err
	}

	return nil
}

// ClearBeforeWatermark removes all entries in the Write-Ahead Log (WAL) before the specified watermark.
// It creates a new WAL file with the remaining entries.
func (w *WAL) Clear() error {
	// Create a new WAL to store the filtered content.
	newWAL, err := NewWAL("disk/walStorage/new_wal.bin")
	if err != nil {
		return err
	}
	defer newWAL.Close()

	// Get the current file size.
	fileInfo, err := w.file.Stat()
	if err != nil {
		return err
	}
	fileSize := fileInfo.Size()

	// If the file is empty, nothing to clear.
	if fileSize == 0 {
		return nil
	}

	// Iterate through the entire WAL file.
	for offset := int64(0); offset < fileSize; {
		entry, nextOffset, watermark, err := readWALEntryAt(w.file, offset)
		if err != nil {
			return err
		}

		// If the watermark is found, write the remaining entries to the new WAL.
		if watermark == WatermarkPlaceholder {
			// This is the watermark entry, write it and subsequent entries.
			if err := newWAL.AppendEntry(Watermark, entry.Operation, entry.Key, entry.Value); err != nil {
				return err
			}
		}

		// Move to the next entry.
		offset = nextOffset
	}

	// Close both the original and new WAL files.
	if err := w.Close(); err != nil {
		return err
	}
	if err := newWAL.Close(); err != nil {
		return err
	}

	// Replace the original WAL with the new one.
	if err := os.Rename("disk/walStorage/new_wal.bin", "disk/walStorage/wal.bin"); err != nil {
		return err
	}

	return nil
}
