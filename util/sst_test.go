package util

import (
	"bytes"
	"os"
	"reflect"
	"testing"
)

func TestNewSSTFile(t *testing.T) {
	res, err := NewSSTFile()
	if err != nil {
		t.Fatalf("Error creating the file: %s", err)
	}
	res.Close()
	if err := os.Remove(res.File.Name()); err != nil {
		t.Fatal(err)
	}
}

func TestReadWriteBinary(t *testing.T) {
	sst, err := NewSSTFile()
	if err != nil {
		t.Fatalf("Error creating the file: %s", err)
	}

	var (
		var1 []byte = []byte{72, 101, 108, 108, 111}
		var2 uint32 = 123456
		var3 uint16 = 12345
		res1 []byte
		res2 uint32
		res3 uint16
	)

	err = writeBinary(sst.File, var1, var2, var3)
	if err != nil {
		t.Fatalf("Error writing the variables: %s", err)
	}
	sst.File.Seek(0, 0)

	res1, err = readBytes(sst.File, 5)
	if err != nil {
		t.Fatalf("Error reading fixed number of bytes: %s", err)
	}

	err = readBinary(sst.File, &res2, &res3)
	if !bytes.Equal(res1, var1) || res2 != var2 || res3 != var3 {
		t.Fatalf("Read unexpected values: %s", err)
	}

	sst.Close()
	if err := os.Remove(sst.File.Name()); err != nil {
		t.Log(err)
	}
}

func TestReadWriteHeader(t *testing.T) {
	var h, res SSTFileHeader
	h.Magic = []byte(magicString)
	h.EntryCount = 2
	h.LongestKey = []byte("fooo")
	h.SmallestKey = []byte("foo")
	h.Version = 3

	sst, err := NewSSTFile()
	if err != nil {
		t.Fatalf("Error creating the file: %s", err)
	}

	err = sst.writeHeader(h)
	if err != nil {
		t.Fatalf("Error writing the header: %s", err)
	}

	sst.File.Seek(0, 0)

	res, err = sst.readHeader()
	if err != nil {
		t.Fatalf("Error reading the header: %s", err)
	}

	if !reflect.DeepEqual(h, res) {
		t.Fatalf("Error: written and read headers are not equal")
	}

	sst.Close()
	if err := os.Remove(sst.File.Name()); err != nil {
		t.Log(err)
	}
}

func TestGet(t *testing.T) {
	sst, err := NewSSTFile()
	if err != nil {
		t.Errorf("Error creating the file: %s", err)
	}

	var key = []byte("foo")

	var p SSTPair
	p.Operation = setOperation
	p.Value = []byte("bar")

	var entry SSTTuple
	entry.Key = key
	entry.Value = p

	var h SSTFileHeader
	h.Magic = []byte(magicString)
	h.EntryCount = 1
	h.LongestKey = entry.Key
	h.SmallestKey = entry.Key
	h.Version = 1

	sst.writeHeader(h)
	sst.writeTuple(entry)

	sst.File.Seek(0, 0)

	res, n := sst.Get(entry.Key)
	if n != 1 {
		t.Fatalf("Error finding key")
	}
	if !bytes.Equal(res, entry.Value.Value) {
		t.Fatalf("Error: retrieved unexpected value")
	}

	sst.Close()
	if err := os.Remove(sst.File.Name()); err != nil {
		t.Fatal(err)
	}
}
