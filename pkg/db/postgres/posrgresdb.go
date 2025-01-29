package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

type Option func(s *sql.DB)

func WithMaxOpenConn(moc int) Option {
	return func(s *sql.DB) {
		s.SetMaxOpenConns(moc)
	}
}

func WithMaxIdleConn(mic int) Option {
	return func(s *sql.DB) {
		s.SetMaxIdleConns(mic)
	}
}

func WithMaxIdleTime(mit time.Duration) Option {
	return func(s *sql.DB) {
		s.SetConnMaxIdleTime(mit)
	}
}

func OpenDB(ctx context.Context, host string, port string, user string,
	password string, dbname string, options ...Option) error {
	psqlInfo := fmt.Sprintf("host=%s port=%s user=%s "+
		"password=%s dbname=%s sslmode=disable",
		host, port, user, password, dbname)

	db, err := sql.Open("postgres", psqlInfo)
	if err != nil {
		panic(err)
	}

	// Apply all options
	for _, option := range options {
		option(db)
	}

	// check if connection is ok.
	err = db.Ping()
	if err != nil {
		db.Close()
		return fmt.Errorf("failed to connect to the database: %v", err)
	}

	return err
}
