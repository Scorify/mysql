package mysql

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"

	_ "github.com/go-sql-driver/mysql"
)

type Schema struct {
	Server   string `key:"target"`
	Port     int    `key:"port" default:"3306"`
	Username string `key:"username"`
	Password string `key:"password"`
	Database string `key:"database"`
	Query    string `key:"query"`
}

func Run(ctx context.Context, config string) error {
	schema := Schema{}

	err := json.Unmarshal([]byte(config), &schema)
	if err != nil {
		return err
	}

	connStr := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s", schema.Username, schema.Password, schema.Target, schema.Port, schema.Database)

	conn, err := sql.Open("mysql", connStr)
	if err != nil {
		return fmt.Errorf("failed to connect to mysql server: %w", err)
	}
	defer conn.Close()

	err = conn.Ping()
	if err != nil {
		return fmt.Errorf("failed to ping mysql server: %w", err)
	}

	if schema.Query != "" {
		rows, err := conn.Query(schema.Query)
		if err != nil {
			return fmt.Errorf("failed to execute query: %w", err)
		}
		defer rows.Close()

		if !rows.Next() {
			return fmt.Errorf("no rows returned from query: %q", schema.Query)
		}
	}

	return nil
}
