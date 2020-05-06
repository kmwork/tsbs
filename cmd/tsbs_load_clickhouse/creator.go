package main

import (
	"bufio"
	"fmt"
	"github.com/timescale/tsbs/internal/utils"
	"log"
	"math"
	"strings"

	"github.com/jmoiron/sqlx"
)

// loader.DBCreator interface implementation
type dbCreator struct {
	tags    string
	cols    []string
	connStr string
}

// loader.DBCreator interface implementation
func (d *dbCreator) Init() {
	log.Println("[DB-Creator:Init] start")
	br := loader.GetBufferedReader()
	d.readDataHeader(br)
	log.Println("[DB-Creator:Init] done")
}

// readDataHeader fills dbCreator struct with data structure (tables description)
// specified at the beginning of the data file
func (d *dbCreator) readDataHeader(br *bufio.Reader) {
	// First N lines are header, describing data structure.
	// The first line containing tags table name ('tags') followed by list of tags, comma-separated.
	// Ex.: tags,hostname,region,datacenter,rack,os,arch,team,service,service_version
	// The second through N-1 line containing table name (ex.: 'cpu') followed by list of column names,
	// comma-separated. Ex.: cpu,usage_user,usage_system,usage_idle,usage_nice,usage_iowait,usage_irq,usage_softirq
	// The last line being blank to separate from the data
	//
	// Header example:
	// tags,hostname,region,datacenter,rack,os,arch,team,service,service_version,service_environment
	// cpu,usage_user,usage_system,usage_idle,usage_nice,usage_iowait,usage_irq,usage_softirq,usage_steal,usage_guest,usage_guest_nice
	// disk,total,free,used,used_percent,inodes_total,inodes_free,inodes_used
	// nginx,accepts,active,handled,reading,requests,waiting,writing

	i := 0
	for {
		var err error
		var line string
		if math.Mod(float64(i), 1000) < 0.001 {
			log.Printf("read, index = %d", i)
		}
		if i == 0 {
			// read first line - list of tags
			d.tags, err = br.ReadString('\n')
			if err != nil {
				fatal("input has wrong header format: %v", err)
			}
			d.tags = strings.TrimSpace(d.tags)
		} else {
			// read the second and further lines - metrics descriptions
			line, err = br.ReadString('\n')
			if err != nil {
				log.Printf("next line , index = %d", i)
				fatal("input has wrong header format: %v", err)
			}
			line = strings.TrimSpace(line)
			if len(line) == 0 {
				// empty line - end of header
				break
			}
			// append new table/columns set to the list of tables/columns set
			d.cols = append(d.cols, line)
		}
		i++
	}
}

// loader.DBCreator interface implementation
func (d *dbCreator) DBExists(dbName string) bool {
	db := sqlx.MustConnect(dbType, getConnectString(false))
	defer db.Close()

	sql := fmt.Sprintf("SELECT name, engine FROM system.databases WHERE name = '%s'", dbName)
	if debug > 0 {
		fmt.Printf(sql)
	}
	var rows []struct {
		Name   string `db:"name"`
		Engine string `db:"engine"`
	}

	err := db.Select(&rows, sql)
	if err != nil {
		panic(err)
	}
	for _, row := range rows {
		if row.Name == dbName {
			return true
		}
	}

	return false
}

// loader.DBCreator interface implementation
func (d *dbCreator) RemoveOldDB(dbName string) error {
	// We do not want to drop DB
	return nil
}

// loader.DBCreator interface implementation
func (d *dbCreator) CreateDB(dbName string) error {
	// Connect to ClickHouse in general and CREATE DATABASE
	db := sqlx.MustConnect(dbType, getConnectString(false))
	sql := fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", dbName)
	_, err := db.Exec(sql)
	if err != nil {
		panic(err)
	}
	db.Close()
	db = nil

	// Connect to specified database within ClickHouse
	db = sqlx.MustConnect(dbType, getConnectString(true))
	defer db.Close()

	// d.tags content:
	//tags,hostname,region,datacenter,rack,os,arch,team,service,service_version,service_environment
	//
	// Parts would contain
	// 0: tags - reserved word - tags mark
	// 1:
	// N: actual tags
	// so we'll use tags[1:] for tags specification

	// d.cols content are lines (metrics descriptions) as:
	// cpu,usage_user,usage_system,usage_idle,usage_nice,usage_iowait,usage_irq,usage_softirq,usage_steal,usage_guest,usage_guest_nice
	// disk,total,free,used,used_percent,inodes_total,inodes_free,inodes_used
	// nginx,accepts,active,handled,reading,requests,waiting,writing
	// generalised description:
	// tableName,fieldName1,...,fieldNameX
	for _, cols := range d.cols {
		// cols content:
		// cpu,usage_user,usage_system,usage_idle,usage_nice,usage_iowait,usage_irq,usage_softirq,usage_steal,usage_guest,usage_guest_nice
		createMetricsTable(db, strings.Split(strings.TrimSpace(cols), ","))
	}

	return nil
}

func (d *dbCreator) PostCreateDB(dbName string) error {
	for _, cols := range d.cols {
		parts := strings.Split(strings.TrimSpace(cols), ",")
		tableCols[parts[0]] = parts[1:]
	}

	return nil
}

// createMetricsTable builds CREATE TABLE SQL statement and runs it
func createMetricsTable(db *sqlx.DB, tableSpec []string) {
	// tableSpec contain
	// 0: table name
	// 1: table column name 1
	// N: table column name N

	// Ex.: cpu OR disk OR nginx
	tableName := tableSpec[0]
	tableCols[tableName] = tableSpec[1:]

	// columnsWithType - column specifications with type. Ex.: "cpu_usage Float64"
	var columnsWithType []string = make([]string, utils.KostyaColumnCounter())
	var i int64
	for i = 0; i < utils.KostyaColumnCounter(); i++ {
		columnsWithType[i] = tableSpec[i+1] + " Float64 CODEC(Delta,LZ4)"
	}

	sql := fmt.Sprintf(`
			CREATE TABLE IF NOT EXISTS %s (
				created_date    Date     DEFAULT today(),
				created_at      DateTime DEFAULT now(),
				id		        UInt64,
				%s
			) ENGINE = MergeTree(created_date, (id, created_at), 8192)
			`,
		tableName,
		strings.Join(columnsWithType, ","))
	_, err := db.Exec(sql)
	if err != nil {
		panic(err)
	}
	truncateTable(db, tableName)
}

func truncateTable(db *sqlx.DB, tableName string) {
	sql := fmt.Sprintf("TRUNCATE TABLE %s", tableName)
	_, err := db.Exec(sql)
	if err != nil {
		panic(err)
	}
}

// getConnectString() builds connect string to ClickHouse
// db - whether database specification should be added to the connection string
func getConnectString(db bool) string {
	// connectString: tcp://127.0.0.1:9000?debug=true
	// ClickHouse ex.:
	// tcp://host1:9000?username=user&password=qwerty&database=clicks&read_timeout=10&write_timeout=20&alt_hosts=host2:9000,host3:9000
	var strUrl string
	if db {
		strUrl = fmt.Sprintf("tcp://%s:%s?username=%s&password=%s&database=%s&compression='lz4'", host, port, user, password, loader.DatabaseName())
	} else {
		strUrl = fmt.Sprintf("tcp://%s:%s?username=%s&password=%s&compression='lz4'", host, port, user, password)
	}
	log.Printf("connection Url = %s, Type_as_db = %t", strUrl, db)
	return strUrl
}
