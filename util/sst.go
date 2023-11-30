package util

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

// SST file struct.
type SSTFile struct {
	File *os.File
}

// SSTFileHeader represents the header of the SST file.
type SSTFileHeader struct {
	Magic       []byte
	EntryCount  uint32
	SmallestKey []byte
	LongestKey  []byte
	Version     uint16
}

// SSTTuple represents a key-value pair in the SST file.
type SSTTuple struct {
	Key   []byte
	Value Value
}

// NewSSTFile creates a new instance of the SST File.
func NewSSTFile() (*SSTFile, error) {
	// Get the directory for SST storage
	sstDir := filepath.Join("disk", "sstStorage")

	// Create the directory if it doesn't exist
	if err := os.MkdirAll(sstDir, os.ModePerm); err != nil {
		return nil, err
	}

	// Find the last created SST file to determine the next number
	lastSSTNumber, err := findLastSSTNumber(sstDir)
	if err != nil {
		return nil, err
	}

	// Generate the filename for the new SST file
	filename := fmt.Sprintf("sst%03d", lastSSTNumber+1)

	// Create the SST file
	file, err := os.Create(filepath.Join(sstDir, filename))
	if err != nil {
		return nil, err
	}

	// Return the SSTFile struct with the opened file
	return &SSTFile{File: file}, nil
}

// Helper function to find the last created SST file number
func findLastSSTNumber(sstDir string) (int, error) {
	// Find all files in the SST storage directory
	files, err := filepath.Glob(filepath.Join(sstDir, "sst*"))
	if err != nil {
		return 0, err
	}

	// Iterate through the files to find the last number
	var lastNumber int
	for _, file := range files {
		_, err := fmt.Sscanf(filepath.Base(file), "sst%03d", &lastNumber)
		if err != nil {
			continue // Ignore files that don't match the naming pattern
		}
	}

	return lastNumber, nil
}

// Close closes the SST file.
func (s *SSTFile) Close() error {
	return s.File.Close()
}

// writeHeader writes the SST file header.
func (s *SSTFile) writeHeader(header SSTFileHeader) error {
	// Magic number.
	if err := binary.Write(s.File, binary.BigEndian, header.Magic); err != nil {
		return err
	}

	// Entry count.
	if err := binary.Write(s.File, binary.BigEndian, header.EntryCount); err != nil {
		return err
	}

	// Smallest key length.
	if err := binary.Write(s.File, binary.BigEndian, uint32(len(header.SmallestKey))); err != nil {
		return err
	}

	// Smallest key.
	if err := binary.Write(s.File, binary.BigEndian, header.SmallestKey); err != nil {
		return err
	}

	// Longest key length.
	if err := binary.Write(s.File, binary.BigEndian, uint32(len(header.LongestKey))); err != nil {
		return err
	}

	// Longest key.
	if err := binary.Write(s.File, binary.BigEndian, header.LongestKey); err != nil {
		return err
	}

	// Version.
	if err := binary.Write(s.File, binary.BigEndian, header.Version); err != nil {
		return err
	}

	return nil
}

// writeTuple writes a key-value pair into the SST file.
func (s *SSTFile) writeTuple(key []byte, value Value) error {
	// Operation
	if err := binary.Write(s.File, binary.BigEndian, []byte(value.Operation)); err != nil {
		return err
	}

	if value.Operation == "SET" {
		// Key length.
		if err := binary.Write(s.File, binary.BigEndian, uint32(len(key))); err != nil {
			return err
		}

		// Key.
		if err := binary.Write(s.File, binary.BigEndian, key); err != nil {
			return err
		}

		// Value length.
		if err := binary.Write(s.File, binary.BigEndian, uint32(len(value.Value))); err != nil {
			return err
		}

		// Value.
		if err := binary.Write(s.File, binary.BigEndian, value.Value); err != nil {
			return err
		}
	} else if value.Operation == "DEL" {
		// Key length.
		if err := binary.Write(s.File, binary.BigEndian, uint32(len(key))); err != nil {
			return err
		}

		// Key.
		if err := binary.Write(s.File, binary.BigEndian, key); err != nil {
			return err
		}
	}

	return nil
}

// GetValueByKey retrieves the value for a given key in the SST file.
func (s *SSTFile) GetValueByKey(key []byte) (string, int, error) {
	var magic [4]byte
	var entryCount uint32
	var smallestKeyLength uint32
	var longestKeyLength uint32
	var version uint16

	// Read and decode the header element by element to get information about the SST file
	if err := binary.Read(s.File, binary.BigEndian, &magic); err != nil {
		return "", 2, err
	}

	if string(magic[:]) != "SSTF" {
		return "", 2, errors.New("Invalid SST file format")
	}

	if err := binary.Read(s.File, binary.BigEndian, &entryCount); err != nil {
		return "", 2, err
	}

	if err := binary.Read(s.File, binary.BigEndian, &smallestKeyLength); err != nil {
		return "", 2, err
	}
	smallestKeyBytes := make([]byte, smallestKeyLength)
	if err := binary.Read(s.File, binary.BigEndian, &smallestKeyBytes); err != nil {
		return "", 2, err
	}

	if err := binary.Read(s.File, binary.BigEndian, &longestKeyLength); err != nil {
		return "", 2, err
	}
	longestKeyBytes := make([]byte, longestKeyLength)
	if err := binary.Read(s.File, binary.BigEndian, &longestKeyBytes); err != nil {
		return "", 2, err
	}

	if err := binary.Read(s.File, binary.BigEndian, &version); err != nil {
		return "", 2, err
	}

	// Iterate through the tuples
	for {
		var operationType [3]byte
		if err := binary.Read(s.File, binary.BigEndian, &operationType); err != nil {
			if err == io.EOF {
				break // End of file reached
			}
			return "", 2, err
		}

		var keyLength uint32
		if err := binary.Read(s.File, binary.BigEndian, &keyLength); err != nil {
			return "", 2, err
		}

		keyBytes := make([]byte, keyLength)
		if _, err := io.ReadFull(s.File, keyBytes); err != nil {
			return "", 2, err
		}

		if string(operationType[:]) == "SET" {
			var valueLength uint32
			if err := binary.Read(s.File, binary.BigEndian, &valueLength); err != nil {
				return "", 2, err
			}

			valueBytes := make([]byte, valueLength)
			if _, err := io.ReadFull(s.File, valueBytes); err != nil {
				return "", 2, err
			}

			if bytes.Equal(key, keyBytes) {
				return string(valueBytes), 1, nil
			}
		} else if string(operationType[:]) == "DEL" {
			if bytes.Equal(key, keyBytes) {
				return "", 0, fmt.Errorf("key '%s' is marked as deleted", key)
			}
		}
	}

	return "", 3, fmt.Errorf("key '%s' not found", key)
}
