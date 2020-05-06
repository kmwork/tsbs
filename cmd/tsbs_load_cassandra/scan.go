package main

import (
	"bufio"
	"fmt"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/timescale/tsbs/load"
)

type decoder struct {
	scanner *bufio.Scanner
}

// Reads and returns a CSV line that encodes a data point.
// Since scanning happens in a single thread, we hold off on transforming it
// to an INSERT statement until it's being processed concurrently by a worker.
func (d *decoder) Decode(_ *bufio.Reader) *load.Point {
	ok := d.scanner.Scan()
	if !ok && d.scanner.Err() == nil { // nothing scanned & no error = EOF
		return nil
	} else if !ok {
		log.Fatalf("scan error: %v", d.scanner.Err())
	}

	return load.NewPoint(d.scanner.Text())
}

// Transforms a CSV string encoding a single metric into a CQL INSERT statement.
// We currently only support a 1-line:1-metric mapping for Cassandra. Implement
// other functions here to support other formats.
func singleMetricToInsertStatement(text string, columnsLine string) string {
	insertStatement := "INSERT INTO cassandra_cpu(cassandra_id %s) VALUES(%s)"
	parts := strings.Split(text, ",")

	id := strconv.FormatInt(int64(time.Now().Nanosecond()), 10)
	valuesLine := id + ", " + strings.Join(parts[2:], ",") // offset: table + numTags + timestamp + measurementName + dayBucket + timestampNS

	result := fmt.Sprintf(insertStatement, columnsLine, valuesLine)
	//log.Printf("[SQL:Insert] result = %s", result)
	return result
}

type eventsBatch struct {
	rows []string
}

func (eb *eventsBatch) Len() int {
	return len(eb.rows)
}

func (eb *eventsBatch) Append(item *load.Point) {
	that := item.Data.(string)
	eb.rows = append(eb.rows, that)
}

var ePool = &sync.Pool{New: func() interface{} { return &eventsBatch{rows: []string{}} }}

type factory struct{}

func (f *factory) New() load.Batch {
	return ePool.Get().(*eventsBatch)
}
