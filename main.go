package main

import (
	"fmt"
	"kvstore/util"
	"os"
)

//"log"
//"net/http"

func main() {
	// server, _ := util.NewServer()
	// server.SetupRoutes()
	// port := 8080
	// fmt.Printf("Server is running on :%d...\n", port)
	// log.Fatal(http.ListenAndServe(fmt.Sprintf(":%d", port), server.Router))

	db, err := util.NewMemDB()
	if err != nil {
		fmt.Println("Error creating MemDB:", err)
		return
	}
	repl := &util.Repl{
		Db:  db,
		In:  os.Stdin,
		Out: os.Stdout,
	}

	repl.Start()
}
