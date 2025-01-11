package postgres

import (
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/henderiw/apiserver-store/pkg/storebackend"
	"github.com/lib/pq"
)

const (
	_SETUP    = "setup.sql"
	qSchemaIf = "CALL check_and_create_schema($1, $2)"
	qTableIf  = "CALL create_table_in_schema($1, $2, $3)"
	qEntry    = "CALL retrieve_data($1, $2, $3, $4, $5)"
	qEntries  = "SELECT * FROM retrieve_data_list($1, $2)"
	qInsert   = "CALL insert_entry($1, $2, $3, $4, $5)"
	qUpdate   = "CALL update_entry($1, $2, $3, $4, $5)"
	qdelete   = "CALL delete_entry($1, $2, $3 ,$4)"
)

var (
	errUnique_key_voilation = errors.New("unique key violation")
)

type pgdb struct {
	db     *sql.DB
	schema string
	table  string
}

func (p *pgdb) Initialize(db *sql.DB, cfg *storebackend.Config) error {
	// check if connection is alive.
	err := db.Ping()
	if err != nil {
		return fmt.Errorf("failed to connect to the database: %v", err)
	}

	p.schema = cfg.GroupResource.Group
	p.table = cfg.GroupResource.Resource
	p.db = db

	err = p.setup()
	if err != nil {
		return fmt.Errorf("unable to setup postgres db for storage. %v", err)
	}

	_, err = p.createGroupSchema()
	if err != nil {
		return fmt.Errorf("unable to setup db schema for group %s. %v", p.schema, err)
	}

	_, err = p.createResourceTable()
	if err != nil {
		return fmt.Errorf("unable to setup table for resource %s. %v", p.table, err)
	}

	return nil
}

func (p *pgdb) setup() error {
	scriptPath := filepath.Join("postgres", _SETUP)
	content, err := os.ReadFile(scriptPath)
	if err != nil {
		return fmt.Errorf("failed to read setup file: %v", err)
	}
	_, err = p.db.Exec(string(content))
	if err != nil {
		return fmt.Errorf("failed to execute setup: %v", err)
	}
	return nil
}

func (p *pgdb) createGroupSchema() (bool, error) {
	var created bool
	err := p.db.QueryRow(qSchemaIf, p.schema, created).Scan(&created)
	if err != nil {
		return false, fmt.Errorf("unable to check pg schema for resource: %v", err)
	}

	return created, nil
}

func (p *pgdb) createResourceTable() (bool, error) {
	var created bool
	err := p.db.QueryRow(qTableIf, p.schema, p.table, created).Scan(&created)
	if err != nil {
		return false, fmt.Errorf("failed to create resource for table %s: %v", p.table, err)
	}
	return created, nil
}

func (p *pgdb) retrieveData(key storebackend.Key, tx *sql.Tx) ([]byte, error) {
	var data []byte
	var err error
	if tx != nil {
		err = tx.QueryRow(qEntry, p.schema, p.table, key.Namespace, key.Name).Scan(&data)
	} else {
		err = p.db.QueryRow(qEntry, p.schema, p.table, key.Namespace, key.Name).Scan(&data)
	}

	if err != nil {
		return nil, fmt.Errorf("failed to call retrieve data for resource: %s name: %s error %v", p.table, key.Name, err)
	}
	return data, nil
}

func (p *pgdb) retrieveDataList(tx *sql.Tx) (*sql.Rows, error) {
	var err error
	var rows *sql.Rows
	if tx != nil {
		rows, err = tx.Query(qEntries, p.schema, p.table)
	} else {
		rows, err = p.db.Query(qEntries, p.schema, p.table)
	}

	if err != nil {
		return nil, fmt.Errorf("failed fetch entries for resource %v: error %v", p.table, err)
	}

	return rows, nil
}

func (p *pgdb) insertEntry(key storebackend.Key, data []byte) error {
	_, err := p.db.Exec(qInsert, p.schema, p.table, key.Namespace, key.Name, data)
	if err != nil {
		if pgErr, ok := err.(*pq.Error); ok {
			switch pgErr.Code {
			case "U0001":
				return errUnique_key_voilation
			default:
				return pgErr
			}
		}
	}
	return nil
}

func (p *pgdb) updateOnConflict(key storebackend.Key, data []byte, tx *sql.Tx) error {
	var err error
	if tx != nil {
		_, err = tx.Exec(qUpdate, p.schema, p.table, key.Namespace, key.Name, data)
	} else {
		_, err = p.db.Exec(qUpdate, p.schema, p.table, key.Namespace, key.Name, data)
	}

	if err != nil {
		return err
	}
	return nil
}

func (p *pgdb) delete_entry(key storebackend.Key) error {
	_, err := p.db.Exec(qdelete, p.schema, p.table, key.Namespace, key.Name)
	if err != nil {
		return err
	}
	return nil
}
