package main

import (
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/joho/godotenv"
)

type Value struct {
	Data      []byte
	ExpiresAt time.Time
}

type Database struct {
	Data map[string]*Value
	Mut  sync.RWMutex
	Key  string
}

var DEFAULT_TTL = 5 * time.Minute

func CreateDatabase() (*Database, error) {
	godotenv.Load()
	k := os.Getenv("DB_KEY")
	if len(k) == 0 {
		return nil, fmt.Errorf("Database key is not provided")
	}
	db := Database{Data: make(map[string]*Value), Key: k}
	return &db, nil
}

func (db *Database) Authenticate(password string) bool {
	return password == db.Key
}

func (db *Database) Set(key string, val []byte) {
	db.Mut.Lock()
	defer db.Mut.Unlock()
	db.Data[key] = &Value{val, time.Now().Add(DEFAULT_TTL)}
}

func (db *Database) Get(key string) []byte {
	db.Mut.Lock()
	defer db.Mut.Unlock()
	val, ok := db.Data[key]
	if !ok || time.Now().After(val.ExpiresAt) {
		delete(db.Data, key)
		return nil
	}
	val.ExpiresAt = time.Now().Add(DEFAULT_TTL)
	return val.Data
}

func (db *Database) Print() []byte {
	db.Mut.RLock()
	defer db.Mut.RUnlock()
	var result []byte
	for k, v := range db.Data {
		line := fmt.Sprintf("%s: %s\n", k, string(v.Data))
		result = append(result, []byte(line)...)
	}
	if len(result) == 0 {
		return []byte("(empty)\n")
	}
	return result
}

func (db *Database) Delete(key string) bool {
	db.Mut.Lock()
	defer db.Mut.Unlock()
	_, ok := db.Data[key]
	delete(db.Data, key)
	return ok
}
