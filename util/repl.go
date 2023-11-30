package util

import (
	"bufio"
	"fmt"
	"io"
	"strings"
)

type Cmd int

const (
	Get Cmd = iota
	Set
	Del
	Ext
	Unk
)

type Error int

func (e Error) Error() string {
	return "Empty command"
}

const (
	Empty Error = iota
)

type Repl struct {
	Db  DB
	In  io.Reader
	Out io.Writer
}

func (re *Repl) parseCmd(buf []byte) (Cmd, []string, error) {
	line := string(buf)
	elements := strings.Fields(line)
	if len(elements) < 1 {
		return Unk, nil, Empty
	}

	switch elements[0] {
	case "get":
		return Get, elements[1:], nil
	case "set":
		return Set, elements[1:], nil
	case "del":
		return Del, elements[1:], nil
	case "exit":
		return Ext, nil, nil
	default:
		return Unk, nil, nil
	}
}

func (re *Repl) Start() {
	scanner := bufio.NewScanner(re.In)
	for {
		fmt.Fprint(re.Out, "> ")
		if !scanner.Scan() {
			break
		}
		buf := scanner.Bytes()
		cmd, elements, err := re.parseCmd(buf)
		if err != nil {
			fmt.Fprintf(re.Out, "%s\n", err.Error())
			continue
		}
		switch cmd {
		case Get:
			if len(elements) != 1 {
				fmt.Fprintf(re.Out, "Expected 1 arguments, received: %d\n", len(elements))
				continue
			}
			v, err := re.Db.Get([]byte(elements[0]))
			if err != nil {
				fmt.Fprintln(re.Out, err.Error())
				continue
			}
			fmt.Fprintln(re.Out, string(v))
		case Set:
			if len(elements) != 2 {
				fmt.Printf("Expected 2 arguments, received: %d\n", len(elements))
				continue
			}
			err := re.Db.Set([]byte(elements[0]), []byte(elements[1]))
			if err != nil {
				fmt.Fprintln(re.Out, err.Error())
				continue
			}
		case Del:
			if len(elements) != 1 {
				fmt.Printf("Expected 1 arguments, received: %d\n", len(elements))
				continue
			}
			v, err := re.Db.Del([]byte(elements[0]))
			if err != nil {
				fmt.Fprintln(re.Out, err.Error())
				continue
			}
			fmt.Fprintln(re.Out, string(v))
		case Ext:
			fmt.Fprintln(re.Out, "Bye!")
			return
		case Unk:
			fmt.Fprintln(re.Out, "Unkown command")
		}
	}

	if err := scanner.Err(); err != nil {
		fmt.Fprintln(re.Out, err.Error())
	} else {
		fmt.Fprintln(re.Out, "Bye!")
	}
}
