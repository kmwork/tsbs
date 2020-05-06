package main

import (
	"fmt"
	"github.com/timescale/tsbs/internal/utils"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jmoiron/sqlx"
	_ "github.com/kshvakov/clickhouse"
	"github.com/timescale/tsbs/load"
)

type syncCSI struct {
	// Map hostname to tags.id for this host
	m     map[string]int64
	mutex *sync.RWMutex
}

func newSyncCSI() *syncCSI {
	return &syncCSI{
		m:     make(map[string]int64),
		mutex: &sync.RWMutex{},
	}
}

// globalSyncCSI is used when data is not hashed by some function to a worker consistently so
// therefore all workers need to know about the same map from hostname -> tags_id
var globalSyncCSI = newSyncCSI()

// subsystemTagsToJSON converts equations as
// a=b
// c=d
// into JSON STRING '{"a": "b", "c": "d"}'
func subsystemTagsToJSON(tags []string) string {
	json := "{"
	for i, t := range tags {
		args := strings.Split(t, "=")
		if i > 0 {
			json += ","
		}
		json += fmt.Sprintf("\"%s\": \"%s\"", args[0], args[1])
	}
	json += "}"
	return json
}

// Process part of incoming data - insert into tables
func (p *processor) processCSI(tableName string, rows []*insertData) uint64 {
	log.Printf("[Insert:SQL] tableName = %s", tableName)
	ret := uint64(0)

	// Prepare column names
	// First columns would be "created_date", "created_at", "time", "tags_id", "additional_tags"
	// Inspite of "additional_tags" being added the last one in CREATE TABLE stmt
	// it goes as a third one here - because we can move columns - they are named
	// and it is easier to keep variable coumns at the end of the list

	// INSERT statement template
	var cols []string = make([]string, utils.KostyaColumnCounter()+1)
	var i int64
	cols[0] = "id"
	for i = 0; i < utils.KostyaColumnCounter(); i++ {
		cols[i+1] = "f" + strconv.FormatInt(i, 10)
	}
	var sql = fmt.Sprintf(`
		INSERT INTO %s (
			%s
		) VALUES (
			%s
		)
		`,
		tableName,
		strings.Join(cols[:], ","),
		strings.Repeat(",?", len(cols))[:]) // We need '?,?,?', but repeat ",?" thus we need to chop off 1-st char

	tx := p.db.MustBegin()
	stmt, err := tx.Prepare(sql)
	var rowCount int64 = int64(len(rows))
	var rowIndex int64
	var values []interface{} = make([]interface{}, utils.KostyaColumnCounter()+1)
	log.Printf("Insert for totalRows = %d", rowCount)
	for rowIndex = 0; rowIndex < rowCount; rowIndex++ {
		var strFields = rows[rowIndex].fields
		var metrics []string = strings.Split(strFields, ",")
		if int64(len(metrics)) != utils.KostyaColumnCounter()+1 {
			log.Panicf("metrics invalid = %d", len(metrics))
		}
		var fieldIndex int64
		values[0] = int64(time.Now().Nanosecond())
		for fieldIndex = 1; fieldIndex <= utils.KostyaColumnCounter(); fieldIndex++ {
			f64, err := strconv.ParseFloat(metrics[fieldIndex], 64)
			if err != nil {
				panic(err)
			}
			values[fieldIndex] = f64
		}
		metrics = nil

		_, err := stmt.Exec(values[:]...)
		if err != nil {
			panic(err)
		}
	}
	err = stmt.Close()
	if err != nil {
		panic(err)
	}

	err = tx.Commit()
	if err != nil {
		panic(err)
	}

	if err != nil {
		panic(err)
	}
	//err = stmt.Close()
	//if err != nil {
	//	panic(err)
	//}

	return ret
}

// load.Processor interface implementation
type processor struct {
	db  *sqlx.DB
	csi *syncCSI
}

// load.Processor interface implementation
func (p *processor) Init(workerNum int, doLoad bool) {
	if doLoad {
		p.db = sqlx.MustConnect(dbType, getConnectString(true))
		if hashWorkers {
			p.csi = newSyncCSI()
		} else {
			p.csi = globalSyncCSI
		}
	}
}

// load.ProcessorCloser interface implementation
func (p *processor) Close(doLoad bool) {
	if doLoad {
		p.db.Close()
	}
}

// load.Processor interface implementation
func (p *processor) ProcessBatch(b load.Batch, doLoad bool) (uint64, uint64) {
	batches := b.(*tableArr)
	rowCnt := 0
	metricCnt := uint64(0)
	for tableName, rows := range batches.m {
		rowCnt += len(rows)
		if doLoad {
			start := time.Now()
			metricCnt += p.processCSI(tableName, rows)

			if logBatches {
				now := time.Now()
				took := now.Sub(start)
				batchSize := len(rows)
				fmt.Printf("BATCH: batchsize %d row rate %f/sec (took %v)\n", batchSize, float64(batchSize)/float64(took.Seconds()), took)
			}
		}
	}
	batches.m = map[string][]*insertData{}
	batches.cnt = 0

	return metricCnt, uint64(rowCnt)
}

func convertBasedOnType(serializedType, value string) interface{} {
	if value == "" {
		return nil
	}

	switch serializedType {
	case "string":
		return value
	case "float32":
		f, err := strconv.ParseFloat(value, 32)
		if err != nil {
			panic(fmt.Sprintf("could not parse '%s' to float32", value))
		}
		return float32(f)
	case "float64":
		f, err := strconv.ParseFloat(value, 64)
		if err != nil {
			panic(fmt.Sprintf("could not parse '%s' to float64", value))
		}
		return f
	case "int64":
		i, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			panic(fmt.Sprintf("could not parse '%s' to int64", value))
		}
		return i
	case "int32":
		i, err := strconv.ParseInt(value, 10, 32)
		if err != nil {
			panic(fmt.Sprintf("could not parse '%s' to int64", value))
		}
		return int32(i)
	default:
		panic(fmt.Sprintf("unrecognized type %s", serializedType))
	}
}
