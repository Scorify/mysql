package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"math"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/scorify/schema"
)

type Schema struct {
	Server   string `key:"target"`
	Port     int    `key:"port" default:"3306"`
	Username string `key:"username"`
	Password string `key:"password"`
	Database string `key:"database"`
	Query    string `key:"query"`
}

func Validate(config string) error {
	conf := Schema{}

	err := schema.Unmarshal([]byte(config), &conf)
	if err != nil {
		return err
	}

	if conf.Server == "" {
		return fmt.Errorf("server is required; got %q", conf.Server)
	}

	if conf.Port <= 0 || conf.Port > 65535 {
		return fmt.Errorf("port is invalid; got %d", conf.Port)
	}

	if conf.Username == "" {
		return fmt.Errorf("username is required; got %q", conf.Username)
	}

	if conf.Password == "" {
		return fmt.Errorf("password is required; got %q", conf.Password)
	}

	if conf.Database == "" {
		return fmt.Errorf("database is required; got %q", conf.Database)
	}

	return nil
}

func Run(ctx context.Context, config string) error {
	conf := Schema{}

	err := schema.Unmarshal([]byte(config), &conf)
	if err != nil {
		return err
	}

	deadline, ok := ctx.Deadline()
	if !ok {
		return fmt.Errorf("context deadline is not set")
	}

	connStr := fmt.Sprintf(
		"%s:%s@tcp(%s:%d)/%s?timeout=%ds",
		conf.Username,
		conf.Password,
		conf.Server,
		conf.Port,
		conf.Database,
		int(math.Floor(time.Until(deadline).Seconds())),
	)

	conn, err := sql.Open("mysql", connStr)
	if err != nil {
		return fmt.Errorf("failed to connect to mysql server: %w", err)
	}
	defer conn.Close()

	conn.SetMaxIdleConns(-1)
	conn.SetMaxOpenConns(1)

	err = conn.PingContext(ctx)
	if err != nil {
		return fmt.Errorf("failed to ping mysql server: %w", err)
	}

	if conf.Query != "" {
		rows, err := conn.QueryContext(ctx, conf.Query)
		if err != nil {
			return fmt.Errorf("failed to execute query: %w", err)
		}
		defer rows.Close()

		if !rows.Next() {
			return fmt.Errorf("no rows returned from query: %q", conf.Query)
		}
	}

	return nil
}
