package sqlhandler

import (
	"database/sql"
	"fmt"
	_ "github.com/lib/pq"
	"log"
)

func InitDB(DSN string) (DB *sql.DB, err error) {

	DB, err = sql.Open("postgres",DSN)
	if err != nil {
		return nil, err
	}

	info := fmt.Sprintf("dsn check success")
	log.Println(info)

	err = DB.Ping()
	if err != nil {
		return nil, err
	}

	info = fmt.Sprintf("database connect success")
	log.Println(info)

	return DB, nil
}

func GetBlockHeight(DB *sql.DB) (blockHeight int, err error) {
	query := `SELECT "number" FROM blocks ORDER BY "number" DESC LIMIT 1;`
	err = DB.QueryRow(query).Scan(&blockHeight)

	return blockHeight, err
}