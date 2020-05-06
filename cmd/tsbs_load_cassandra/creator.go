package main

import (
	"fmt"
	"github.com/timescale/tsbs/internal/utils"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/gocql/gocql"
)

type dbCreator struct {
	globalSession *gocql.Session
	clientSession *gocql.Session
}

func (d *dbCreator) Init() {
	cluster := gocql.NewCluster(strings.Split(hosts, ",")...)
	cluster.Consistency = consistencyMapping[consistencyLevel]
	cluster.ProtoVersion = 4
	cluster.Timeout = 10 * time.Second
	session, err := cluster.CreateSession()
	if err != nil {
		log.Fatal(err)
	}
	d.globalSession = session
}

func (d *dbCreator) DBExists(dbName string) bool {
	iter := d.globalSession.Query(fmt.Sprintf("SELECT keyspace_name FROM system_schema.keyspaces;")).Iter()
	defer iter.Close()
	row := ""
	for iter.Scan(&row) {
		if row == dbName {
			return true
		}
	}
	return false
}

func (d *dbCreator) RemoveOldDB(dbName string) error {
	if err := d.globalSession.Query(fmt.Sprintf("drop keyspace if exists %s;", dbName)).Exec(); err != nil {
		return err
	}
	return nil
}

func (d *dbCreator) CreateDB(dbName string) error {
	defer d.globalSession.Close()
	replicationConfiguration := fmt.Sprintf("{ 'class': 'SimpleStrategy', 'replication_factor': %d }", replicationFactor)
	if err := d.globalSession.Query(fmt.Sprintf("create keyspace %s with replication = %s;", dbName, replicationConfiguration)).Exec(); err != nil {
		return err
	}
	var columnsWithType []string = make([]string, utils.KostyaColumnCounter())
	var i int64
	for i = 0; i < utils.KostyaColumnCounter(); i++ {
		columnsWithType[i] = "f" + strconv.FormatInt(i, 10) + " float"
	}

	q := fmt.Sprintf(`CREATE TABLE %s.cassandra_cpu (
					series_id text,
					timestamp_ns bigint,
					%s,
					PRIMARY KEY (series_id, timestamp_ns)
				 )
				 WITH COMPACT STORAGE;`,
		dbName, columnsWithType)

	if err := d.globalSession.Query(q).Exec(); err != nil {
		return err
	}
	return nil
}

func (d *dbCreator) PostCreateDB(dbName string) error {
	cluster := gocql.NewCluster(strings.Split(hosts, ",")...)
	cluster.Keyspace = dbName
	cluster.Timeout = writeTimeout
	cluster.Consistency = consistencyMapping[consistencyLevel]
	cluster.ProtoVersion = 4
	session, err := cluster.CreateSession()
	if err != nil {
		return err
	}
	d.clientSession = session
	return nil
}

func (d *dbCreator) Close() {
	d.clientSession.Close()
}
