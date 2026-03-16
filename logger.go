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

type Logger struct {
	file *os.File
}

func CreateLogger(fp string) (*Logger, error) {
	f, err := os.OpenFile(fp, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, err
	}
	return &Logger{file: f}, nil
}

func (l *Logger) Append(line string) error {
	_, err := fmt.Fprintln(l.file, line)
	if err != nil {
		return err
	}
	return l.file.Sync()
}

func (l *Logger) Restore(db *Database) error {
	f, err := os.Open(l.file.Name())
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
	os.Truncate(l.file.Name(), 0)
	return nil
}

func (l *Logger) Close() error {
	return l.file.Close()
}
