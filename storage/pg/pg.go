package pg

import (
	"database/sql"
	"fmt"
	"io"
	"log"
	"strings"
	"time"

	"github.com/SparkPost/gopg"
	"github.com/SparkPost/httpdump/storage"
	"github.com/lib/pq"
)

type PgDumper struct {
	Schema string
	Dbh    *sql.DB
}

func SchemaInit(dbh *sql.DB, schema string) error {
	if schema == "" {
		schema = "request_dump"
	}
	if strings.Index(schema, " ") >= 0 {
		return fmt.Errorf("schemas containing a space are not supported")
	}

	// initialize schema where request data will be stored
	exists, err := gopg.SchemaExists(dbh, schema)
	if err != nil {
		return err
	}
	if exists == false {
		log.Printf("pg.SchemaInit: creating schema [%s]\n", schema)
		_, err := dbh.Exec(fmt.Sprintf("CREATE SCHEMA %s", pq.QuoteIdentifier(schema)))
		if err != nil {
			return fmt.Errorf("pg.SchemaInit: %s", err)
		}
	}

	table := "raw_requests"
	exists, err = gopg.TableExistsInSchema(dbh, table, schema)
	if err != nil {
		return err
	}
	if exists == false {
		log.Printf("pg.SchemaInit: creating table [%s.%s]\n", schema, table)
		ddls := []string{
			fmt.Sprintf(`
				CREATE TABLE %s.%s (
					request_id bigserial primary key,
					head       text,
					data       text,
					"when"     timestamptz,
					batch_id   bigint
				)
			`, pq.QuoteIdentifier(schema), table),
			fmt.Sprintf("CREATE INDEX raw_requests_batch_id_idx ON %s.%s (batch_id)",
				schema, table),
		}
		for _, ddl := range ddls {
			_, err := dbh.Exec(ddl)
			if err != nil {
				return fmt.Errorf("pg.SchemaInit: %s", err)
			}
		}
	}

	return nil
}

func (pd *PgDumper) Dump(req *storage.Request) error {
	_, err := pd.Dbh.Exec(fmt.Sprintf(`
		INSERT INTO %s.raw_requests (head, data, "when")
		VALUES ($1, $2, $3)
	`, pd.Schema), string(req.Head), string(req.Data), req.When.Format(time.RFC3339))
	if err != nil {
		return fmt.Errorf("pg.Dump (INSERT): %s", err)
	}
	return nil
}

func (pd *PgDumper) MarkBatch() (int64, error) {
	var maxID sql.NullInt64
	row := pd.Dbh.QueryRow(fmt.Sprintf(`
		SELECT max(request_id) FROM %s.raw_requests
		 WHERE (batch_id = 0 OR batch_id IS NULL)
	`, pd.Schema))
	err := row.Scan(&maxID)
	if err != nil {
		return 0, fmt.Errorf("pg.MarkBatch (SELECT): %s", err)
	}
	if maxID.Valid == false {
		return 0, nil
	}

	res, err := pd.Dbh.Exec(fmt.Sprintf(`
		UPDATE %s.raw_requests SET batch_id = $1
		 WHERE (batch_id = 0 OR batch_id IS NULL)
		   AND request_id <= $1`, pd.Schema), maxID.Int64)
	if err != nil {
		return 0, fmt.Errorf("pg.MarkBatch (UPDATE): %s", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return 0, err
	} else if n <= 0 {
		return 0, nil
	}

	return maxID.Int64, nil
}

func (pd *PgDumper) ReadRequests(batchID int64) ([]storage.Request, error) {
	reqs := make([]storage.Request, 0, 32)
	n := 0

	rows, err := pd.Dbh.Query(fmt.Sprintf(`
		SELECT request_id, head, data, "when"
		  FROM %s.raw_requests
		 WHERE batch_id = $1
		 ORDER BY "when" ASC
	`, pd.Schema), batchID)
	if err != nil {
		return nil, fmt.Errorf("pg.ReadRequests (SELECT): %s", err)
	}
	defer rows.Close()

	var tmpID int64
	for rows.Next() {
		if rows.Err() == io.EOF {
			break
		}
		req := &storage.Request{}
		err = rows.Scan(&tmpID, &req.Head, &req.Data, &req.When)
		if err != nil {
			return nil, fmt.Errorf("pg.ReadRequests (Scan): %s", err)
		}
		req.ID = &tmpID
		reqs = append(reqs, *req)
		n++
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("pg.ReadRequests (Err): %s", err)
	}

	return reqs, nil
}

func (pd *PgDumper) BatchDone(batchID int64) error {
	_, err := pd.Dbh.Exec(fmt.Sprintf(`
		DELETE FROM %s.raw_requests WHERE batch_id = $1
	`, pd.Schema), batchID)
	if err != nil {
		return fmt.Errorf("pg.BatchDone (DELETE): %s", err)
	}
	return nil
}
