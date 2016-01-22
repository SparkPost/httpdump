package pg

import (
	"database/sql"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/SparkPost/gopg"
	"github.com/SparkPost/httpdump/storage"
	"github.com/lib/pq"
)

type PgDumper struct {
	Db     string
	Schema string
	Table  string
	User   string
	Pass   string
	dbh    *sql.DB
}

func DbConnect(pg *PgDumper) error {
	cfg := &gopg.Config{
		Db:   pg.Db,
		User: pg.User,
		Pass: pg.Pass,
		Opts: map[string]string{
			"sslmode": "disable",
		},
	}
	dbh, err := gopg.Connect(cfg)
	if err != nil {
		return err
	}

	if err = dbh.Ping(); err != nil {
		return err
	}

	if pg.Schema == "" {
		pg.Schema = "request_dump"
	}
	if strings.Index(pg.Schema, " ") >= 0 {
		return fmt.Errorf("schemas containing a space are not supported")
	}

	// skip creation when schema already exists
	exists := false
	row := dbh.QueryRow(`
		SELECT EXISTS(
			SELECT 1 FROM information_schema.schemata
			 WHERE schema_name = $1
		)`, pg.Schema)
	err = row.Scan(&exists)
	if err != nil {
		return err
	}
	pg.Schema = pq.QuoteIdentifier(pg.Schema)

	// initialize schema where request data will be stored
	if exists == false {
		ddls := []string{
			fmt.Sprintf("CREATE SCHEMA %s", pg.Schema),
			fmt.Sprintf(`
				CREATE TABLE %s.raw_requests (
					request_id bigserial primary key,
					head       text,
					data       text,
					"when"     timestamptz,
					batch_id   bigint
				)
			`, pg.Schema),
			fmt.Sprintf("CREATE INDEX raw_requests_batch_id_idx ON %s.raw_requests (batch_id)", pg.Schema),
		}
		for _, ddl := range ddls {
			_, err := dbh.Exec(ddl)
			if err != nil {
				return err
			}
		}
	}

	pg.dbh = dbh

	return nil
}

func (pd *PgDumper) Dump(req *storage.Request) error {
	_, err := pd.dbh.Exec(fmt.Sprintf(`
		INSERT INTO %s.raw_requests (head, data, "when")
		VALUES ($1, $2, $3)
	`, pd.Schema), string(req.Head), string(req.Data), req.When.Format(time.RFC3339))
	if err != nil {
		return err
	}
	return nil
}

func (pd *PgDumper) MarkBatch() (int64, error) {
	var maxID sql.NullInt64
	row := pd.dbh.QueryRow(fmt.Sprintf(`
		SELECT max(request_id) FROM %s.raw_requests
		 WHERE (batch_id = 0 OR batch_id IS NULL)
	`, pd.Schema))
	err := row.Scan(&maxID)
	if err != nil {
		return 0, err
	}
	if maxID.Valid == false {
		return 0, nil
	}

	res, err := pd.dbh.Exec(fmt.Sprintf(`
		UPDATE %s.raw_requests SET batch_id = $1
		 WHERE (batch_id = 0 OR batch_id IS NULL)
		   AND request_id <= $1`, pd.Schema), maxID.Int64)
	if err != nil {
		return 0, err
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

	rows, err := pd.dbh.Query(fmt.Sprintf(`
		SELECT request_id, head, data, "when"
		  FROM %s.raw_requests
		 WHERE batch_id = $1
		 ORDER BY "when" ASC
	`, pd.Schema), batchID)
	if err != nil {
		return nil, err
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
			return nil, err
		}
		req.ID = &tmpID
		reqs = append(reqs, *req)
		n++
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return reqs, nil
}

func (pd *PgDumper) BatchDone(batchID int64) error {
	_, err := pd.dbh.Exec(fmt.Sprintf(`
		DELETE FROM %s.raw_requests WHERE batch_id = $1
	`, pd.Schema), batchID)
	if err != nil {
		return err
	}
	return nil
}
