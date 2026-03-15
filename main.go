package main

import (
	"bufio"
	"log"
	"net"
	"strings"
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
	for {
		conn, err := listener.Accept()
		if err != nil {
			continue
		}

		go handleConnection(conn, db)
	}
}

func handleConnection(conn net.Conn, db *Database) {
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
			db.Set(parts[1], []byte(parts[2]))
		case "DELETE":
			db.Delete(parts[1])
		case "PRINT":
			conn.Write(db.Print())
		}

	}
}
