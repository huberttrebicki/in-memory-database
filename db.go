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
	Data map[string]Value
	mut  sync.RWMutex
	Key  string
}

var DEFAULT_TTL = 5 * time.Minute

func CreateDatabase() (*Database, error) {
	godotenv.Load()
	k := os.Getenv("DB_KEY")
	if len(k) == 0 {
		return nil, fmt.Errorf("Database key is not provided")
	}
	db := Database{Data: make(map[string]Value), Key: k}
	go db.cleanup(1 * time.Minute)
	return &db, nil
}

func (db *Database) Authenticate(password string) bool {
	return password == db.Key
}

func (db *Database) Set(key string, val []byte) {
	db.mut.Lock()
	defer db.mut.Unlock()
	db.Data[key] = Value{val, time.Now().Add(DEFAULT_TTL)}
}

func (db *Database) Get(key string) []byte {
	db.mut.Lock()
	defer db.mut.Unlock()
	val, ok := db.Data[key]
	if !ok || time.Now().After(val.ExpiresAt) {
		delete(db.Data, key)
		return nil
	}
	val.ExpiresAt = time.Now().Add(DEFAULT_TTL)
	return val.Data
}

func (db *Database) Print() []byte {
	db.mut.RLock()
	defer db.mut.RUnlock()
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

func (db *Database) cleanup(interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for range ticker.C {
		db.mut.Lock()
		now := time.Now()
		for k, v := range db.Data {
			if now.After(v.ExpiresAt) {
				delete(db.Data, k)
			}
		}
		db.mut.Unlock()
	}
}

func (db *Database) Delete(key string) bool {
	db.mut.Lock()
	defer db.mut.Unlock()
	_, ok := db.Data[key]
	delete(db.Data, key)
	return ok
}
