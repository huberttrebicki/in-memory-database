package main

import (
	"bufio"
	"encoding/base64"
	"fmt"
	"log"
	"net"
	"strings"
	"time"
)

func main() {
	listener, err := net.Listen("tcp", ":42069")
	if err != nil {
		log.Fatal("Error while creating a tcp listener on port 42069")
	}
	log.Print("Server successfully started on port 42069")

	db, err := CreateDatabase()
	if err != nil {
		log.Fatalf("Error occured while creating a database instance %v", err)
	}
	persist, err := CreatePersistence()
	if err != nil {
		log.Fatalf("Error occured while creating persistence instance %v", err)
	}
	err = persist.Restore(db)
	if err != nil {
		log.Fatalf("Error occured while restoring database state %v", err)
	}
	go persist.CreateSnapshot(time.NewTicker(5*time.Minute), db)
	for {
		conn, err := listener.Accept()
		if err != nil {
			continue
		}

		go handleConnection(conn, db, persist)
	}
}

func handleConnection(conn net.Conn, db *Database, persist *Persistence) {
	defer conn.Close()
	scannner := bufio.NewScanner(conn)

	if !scannner.Scan() {
		return
	}

	parts := strings.SplitN(scannner.Text(), " ", 2)
	if len(parts) != 2 || strings.ToUpper(parts[0]) != "AUTH" || !db.Authenticate(parts[1]) {
		conn.Write([]byte("Invalid Key\n"))
		return
	}
	conn.Write([]byte("Authenticated successfully\n"))

	for scannner.Scan() {
		parts := strings.Split(scannner.Text(), " ")
		if len(parts) > 3 {
			conn.Write([]byte("ERR Invalid number of arguments\n"))
			continue
		}
		cmd := strings.ToUpper(parts[0])
		switch cmd {
		case "GET":
			val := db.Get(parts[1])
			if val == nil {
				conn.Write([]byte("NULL\n"))
			} else {
				conn.Write(append(val, '\n'))
			}
		case "SET":
			if len(parts) != 3 {
				conn.Write([]byte("INVALID NUMBER OF ARGUMENTS\n"))
			}
			db.Set(parts[1], []byte(parts[2]))
			line := fmt.Sprintf("%d SET %s %s", time.Now().UnixNano(), parts[1], base64.StdEncoding.EncodeToString([]byte(parts[2])))
			persist.Append(line)
		case "DELETE":
			if len(parts) != 2 {
				conn.Write([]byte("INVALID NUMBER OF ARGUMENTS\n"))
				continue
			}
			ok := db.Delete(parts[1])
			if !ok {
				conn.Write([]byte("NOT FOUND\n"))
			} else {
				conn.Write(fmt.Appendf(nil, "DELETED KEY: %v\n", parts[1]))
				line := fmt.Sprintf("%d DELETE %s", time.Now().UnixNano(), parts[1])
				persist.Append(line)
			}
		case "PRINT":
			conn.Write(db.Print())
		}

	}
}
