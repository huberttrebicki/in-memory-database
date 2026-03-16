package main

import (
	"bufio"
	"encoding/base64"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

const (
	logFilePath      = "database.log"
	snapshotFilePath = "snapshot.dat"
)

type Persistence struct {
	logFile *os.File
}

func CreatePersistence() (*Persistence, error) {
	f, err := os.OpenFile(logFilePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, err
	}
	return &Persistence{logFile: f}, nil
}

func (p *Persistence) Append(line string) error {
	_, err := fmt.Fprintln(p.logFile, line)
	if err != nil {
		return err
	}
	return p.logFile.Sync()
}

func (p *Persistence) Restore(db *Database) error {
	if err := p.loadSnapshot(db); err != nil {
		return err
	}
	return p.replayLog(db)
}

func (p *Persistence) loadSnapshot(db *Database) error {
	f, err := os.Open(snapshotFilePath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	defer f.Close()

	now := time.Now()
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		parts := strings.SplitN(scanner.Text(), " ", 3)
		if len(parts) < 3 {
			continue
		}

		ts, err := strconv.ParseInt(parts[0], 10, 64)
		if err != nil {
			continue
		}

		expiresAt := time.Unix(0, ts)
		if now.After(expiresAt) {
			continue
		}

		key := parts[1]
		value, err := base64.StdEncoding.DecodeString(parts[2])
		if err != nil {
			continue
		}

		db.Data[key] = &Value{Data: value, ExpiresAt: expiresAt}
	}
	return scanner.Err()
}

func (p *Persistence) replayLog(db *Database) error {
	f, err := os.Open(p.logFile.Name())
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	defer f.Close()

	now := time.Now()
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		parts := strings.SplitN(scanner.Text(), " ", 4)
		if len(parts) < 2 {
			continue
		}

		ts, err := strconv.ParseInt(parts[0], 10, 64)
		if err != nil {
			continue
		}

		timestamp := time.Unix(0, ts)
		cmd := parts[1]
		switch cmd {
		case "SET":
			if len(parts) < 4 {
				continue
			}
			key := parts[2]
			value, err := base64.StdEncoding.DecodeString(parts[3])
			if err != nil {
				continue
			}
			expiresAt := timestamp.Add(DEFAULT_TTL)
			if now.Before(expiresAt) {
				db.Data[key] = &Value{Data: value, ExpiresAt: expiresAt}
			}
		case "DELETE":
			if len(parts) < 3 {
				continue
			}
			delete(db.Data, parts[2])
		}
	}
	if err := scanner.Err(); err != nil {
		return err
	}
	os.Truncate(p.logFile.Name(), 0)
	return nil
}

func (p *Persistence) CreateSnapshot(t *time.Ticker, db *Database) {
	for range t.C {
		db.Mut.Lock()

		f, err := os.CreateTemp("", "snapshot-*.tmp")
		if err != nil {
			db.Mut.Unlock()
			continue
		}

		for k, v := range db.Data {
			fmt.Fprintf(f, "%d %s %s\n", v.ExpiresAt.UnixNano(), k, base64.StdEncoding.EncodeToString(v.Data))
		}
		f.Sync()
		f.Close()

		os.Rename(f.Name(), snapshotFilePath)
		os.Truncate(p.logFile.Name(), 0)

		db.Mut.Unlock()
	}
}

func (p *Persistence) Close() error {
	return p.logFile.Close()
}
