package util

import (
	"bytes"
	"os"
	"testing"
)

func TestNewSSTFile(t *testing.T) {
	res, err := NewSSTFile()
	if err != nil {
		t.Fatalf("Error creating the file: %s", err)
	}
	res.Close()
	if err := os.Remove(res.File.Name()); err != nil {
		t.Log(err)
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
		t.Errorf("Error reading fixed number of bytes: %s", err)
	}

	err = readBinary(sst.File, &res2, &res3)
	if !bytes.Equal(res1, var1) || res2 != var2 || res3 != var3 {
		t.Errorf("Read unexpected values: %s", err)
	}
}
