package pg

import (
	"database/sql"
	"fmt"
	"regexp"
	"strings"
)

type PGConfig struct {
	Db   string
	User string
	Pass string
	Opts map[string]string
	Url  string
}

var whitespace *regexp.Regexp = regexp.MustCompile(`\s`)

func (cfg *PGConfig) Connect() (*sql.DB, error) {
	var dsn string
	if cfg.Url != "" {
		dsn = cfg.Url

	} else {
		opts := make([]string, 0, 3)

		if cfg.Db != "" {
			if whitespace.MatchString(cfg.Db) {
				return nil, fmt.Errorf("whitespace in database names is not supported")
			}
			opts = append(opts, fmt.Sprintf("dbname=%s", cfg.Db))
		}

		if cfg.User != "" {
			if whitespace.MatchString(cfg.User) {
				return nil, fmt.Errorf("whitespace in user names is not supported")
			}
			opts = append(opts, fmt.Sprintf("user=%s", cfg.User))
		}

		if cfg.Pass != "" {
			if whitespace.MatchString(cfg.Pass) {
				return nil, fmt.Errorf("whitespace in passwords is not supported")
			}
			opts = append(opts, fmt.Sprintf("password=%s", cfg.Pass))
		}

		for k, v := range cfg.Opts {
			if whitespace.MatchString(k) {
				return nil, fmt.Errorf("whitespace in option names is not supported: [%s]", k)
			} else if whitespace.MatchString(v) {
				return nil, fmt.Errorf("whitespace in option values is not supported: [%s] (%s)", v, k)
			}
			opts = append(opts, fmt.Sprintf("%s=%s", k, v))
		}

		dsn = strings.Join(opts, " ")
	}

	dbh, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, err
	}

	return dbh, nil
}

func SchemaExists(dbh *sql.DB, schema string) (bool, error) {
	exists := false
	row := dbh.QueryRow(`
		SELECT EXISTS(
			SELECT 1 FROM information_schema.schemata
			 WHERE schema_name = $1
		)`, schema)
	err := row.Scan(&exists)
	if err != nil {
		return false, err
	}
	return exists, nil
}

func TableExistsInSchema(dbh *sql.DB, table, schema string) (bool, error) {
	exists := false
	row := dbh.QueryRow(`
		SELECT EXISTS(
			SELECT 1 FROM information_schema.tables
			 WHERE table_schema = $1
			   AND table_name = $2
		)`, schema, table)
	err := row.Scan(&exists)
	if err != nil {
		return false, err
	}
	return exists, nil
}
