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

const (
	sstDir       = "../disk/sstStorage"
	magicString  = "SSTF"
	getOperatuon = "GET"
	setOperation = "SET"
	delOperation = "DEL"
)

// SSTFile represents an SST (Sorted String Table) file.
type SSTFile struct {
	File *os.File
}

type SSTFileHeader struct {
	Magic       []byte
	EntryCount  uint32
	SmallestKey []byte
	LongestKey  []byte
	Version     uint16
}

type SSTPair struct {
	Operation string
	Value     []byte
}
type SSTTuple struct {
	Key   []byte
	Value SSTPair
}

// findLastSSTNumber finds the number of the latest SST file created.
func findLastSSTNumber(sstDir string) int {
	files, err := filepath.Glob(filepath.Join(sstDir, "sst*"))
	if err != nil {
		return -1
	}

	var res, num int
	for _, file := range files {
		if _, err := fmt.Sscanf(filepath.Base(file), "sst%03d", &num); err == nil {
			if num > res {
				res = num
			}
		}
	}

	return res
}

func NewSSTFile() (*SSTFile, error) {
	if err := os.MkdirAll(sstDir, os.ModePerm); err != nil {
		return nil, err
	}

	// Find the last SST file number to create a new one
	lastSST := findLastSSTNumber(sstDir)
	if lastSST == -1 {
		return nil, errors.New("Error finding last SST")
	}

	// Generate the new SST file name
	filename := fmt.Sprintf("sst%03d", lastSST+1)

	// Create the new SST file
	file, err := os.Create(filepath.Join(sstDir, filename))
	if err != nil {
		return nil, err
	}

	return &SSTFile{File: file}, nil
}

func (s *SSTFile) Close() error {
	return s.File.Close()
}

// writeBinary writes multiple values into the binary file.
func writeBinary(w io.Writer, values ...interface{}) error {
	for _, value := range values {
		if err := binary.Write(w, binary.BigEndian, value); err != nil {
			return err
		}
	}
	return nil
}

// readBinary reads multiple values from the binary file.
func readBinary(r io.Reader, values ...interface{}) error {
	for _, value := range values {
		if err := binary.Read(r, binary.BigEndian, value); err != nil {
			return err
		}
	}
	return nil
}

// readBytes reads a specified number of bytes from the binary file.
func readBytes(r io.Reader, n int) ([]byte, error) {
	bytes := make([]byte, n)
	_, err := io.ReadFull(r, bytes)
	return bytes, err
}

// readKeyValue reads a key or value from the binary file.
func readKeyValue(r io.Reader) ([]byte, error) {
	var length uint32
	if err := binary.Read(r, binary.BigEndian, &length); err != nil {
		return nil, err
	}
	return readBytes(r, int(length))
}

// readHeader reads the SST file header.
func (s *SSTFile) readHeader() (SSTFileHeader, error) {
	var (
		header SSTFileHeader
		err    error
	)

	header.Magic, err = readBytes(s.File, len(magicString))
	if err != nil {
		return SSTFileHeader{}, err
	}
	err = readBinary(s.File, &header.EntryCount)
	if err != nil {
		return SSTFileHeader{}, err
	}
	header.SmallestKey, err = readKeyValue(s.File)
	if err != nil {
		return SSTFileHeader{}, err
	}
	header.LongestKey, err = readKeyValue(s.File)
	if err != nil {
		return SSTFileHeader{}, err
	}
	err = readBinary(s.File, &header.Version)
	if err != nil {
		return SSTFileHeader{}, err
	}

	return header, nil
}

// writeHeader writes the SST file header.
func (s *SSTFile) writeHeader(header SSTFileHeader) error {
	return writeBinary(s.File, header.Magic, header.EntryCount, uint32(len(header.SmallestKey)), header.SmallestKey, uint32(len(header.LongestKey)), header.LongestKey, header.Version)
}

// writeTuple writes a key-value pair into the SST file.
func (s *SSTFile) writeTuple(entry SSTTuple) error {
	switch entry.Value.Operation {
	case setOperation:
		return writeBinary(s.File, []byte(setOperation), uint32(len(entry.Key)), entry.Key, uint32(len(entry.Value.Value)), entry.Value.Value)
	case delOperation:
		return writeBinary(s.File, []byte(delOperation), uint32(len(entry.Key)), entry.Key)
	default:
		return fmt.Errorf("unsupported operation: %s", entry.Value.Operation)
	}
}

// Get retrieves the value for a given key in the SST file.
func (s *SSTFile) Get(key []byte) ([]byte, int, error) {
	_, err := s.readHeader()
	if err != nil {
		return nil, 2, err
	}

	for {
		opType, err := readBytes(s.File, 3)
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, 2, err
		}

		keyBytes, err := readKeyValue(s.File)
		if err != nil {
			return nil, 2, err
		}

		switch string(opType) {
		case setOperation:
			value, err := readKeyValue(s.File)
			if err != nil {
				return nil, 2, err
			}
			if bytes.Equal(key, keyBytes) {
				return value, 1, nil
			}
		case delOperation:
			if bytes.Equal(key, keyBytes) {
				return nil, 0, fmt.Errorf("key '%s' is marked as deleted", key)
			}
		}
	}

	return nil, 3, fmt.Errorf("key '%s' not found", key)
}
