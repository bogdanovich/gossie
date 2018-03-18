// Mock in-memory implementation for gossie. use NewMockConnectionPool to
// create a gossie.ConnectionPool that stores Batch() mutations in an internal
// map[string][]*gossie.Row.
//
// TODO:
//   - Not all methods are implemented
//   - Change the internal map key from string to []byte
//
// Warning: API not finalized, subject to change.
//
// See the example.
package mockgossie

import (
	"bytes"
	"crypto/md5"
	"math/big"
	"sort"

	"github.com/bogdanobich/go-thrift/thrift"
	. "github.com/bogdanovich/gossie/src/cassandra"
	. "github.com/bogdanovich/gossie/src/gossie"
)

type MockConnectionPool struct {
	Data map[string]Rows
}

var _ ConnectionPool = &MockConnectionPool{}

func NewMockConnectionPool() *MockConnectionPool {
	return &MockConnectionPool{
		Data: make(map[string]Rows),
	}
}

func (*MockConnectionPool) Keyspace() string { return "MockKeyspace" }
func (*MockConnectionPool) Schema() *Schema  { panic("Schema Not Implemented") }
func (m *MockConnectionPool) Reader() Reader { return newReader(m) }
func (m *MockConnectionPool) Writer() Writer { return newWriter(m) }
func (m *MockConnectionPool) Batch() Batch   { return newBatch(m) }
func (*MockConnectionPool) Close()           {}

func (m *MockConnectionPool) WithTracer(Tracer) ConnectionPool { return m }

func (m *MockConnectionPool) Query(mapping Mapping) Query {
	return &MockQuery{
		pool:        m,
		mapping:     mapping,
		rowLimit:    DEFAULT_ROW_LIMIT,
		columnLimit: DEFAULT_COLUMN_LIMIT,
	}
}

type Rows []*Row

func (r Rows) Len() int           { return len(r) }
func (r Rows) Less(i, j int) bool { return compareRowKeys(r[i].Key, r[j].Key) < 0 }
func (r Rows) Swap(i, j int)      { r[i], r[j] = r[j], r[i] }

type Columns []*Column

func (r Columns) Len() int           { return len(r) }
func (r Columns) Less(i, j int) bool { return bytes.Compare(r[i].Name, r[j].Name) < 0 }
func (r Columns) Swap(i, j int)      { r[i], r[j] = r[j], r[i] }

func (m *MockConnectionPool) Rows(cf string) Rows {
	rows, ok := m.Data[cf]
	if !ok {
		rows = make([]*Row, 0)
		m.Data[cf] = rows
	}
	return rows
}

type RowDump map[string][]byte
type CFDump map[string]RowDump
type Dump map[string]CFDump

// Utility method to make validating tests easier
// result is: map[cf]map[rowKey]map[columnName]columnValue
func (m *MockConnectionPool) Dump() Dump {
	d := Dump{}

	for cf, _ := range m.Data {
		d[cf] = m.DumpCF(cf)
	}

	return d
}

// Utility method to make validating tests easier
// result is: map[rowKey]map[columnName]columnValue
func (m *MockConnectionPool) DumpCF(cf string) CFDump {
	d := CFDump{}

	rows, ok := m.Data[cf]
	if !ok {
		return d
	}

	for _, row := range rows {
		rowMap := map[string][]byte{}
		d[string(row.Key)] = rowMap
		for _, column := range row.Columns {
			rowMap[string(column.Name)] = column.Value
		}
	}

	return d
}

// Utility method for loading data in for tests
func (m *MockConnectionPool) Load(dump Dump) {
	for cf, d := range dump {
		m.LoadCF(cf, d)
	}
}

// Utility method for loading data in for tests
func (m *MockConnectionPool) LoadCF(cf string, dump CFDump) {
	rows := []*Row{}
	t := thrift.Int64Ptr(now())
	for key, columns := range dump {
		cols := Columns{}

		for name, value := range columns {
			cols = append(cols, &Column{
				Name:      []byte(name),
				Value:     value,
				Timestamp: t,
			})
		}
		sort.Sort(cols)

		row := &Row{
			Key:     []byte(key),
			Columns: cols,
		}

		// Insert row in sorted order
		i := sort.Search(len(rows), func(i int) bool { return compareRowKeys(rows[i].Key, row.Key) >= 0 })
		rows = append(rows, row)
		copy(rows[i+1:], rows[i:])
		rows[i] = row
	}
	m.Data[cf] = rows
}

func compareRowKeys(a, b []byte) int {
	tokenA := keyToToken(a)
	tokenB := keyToToken(b)

	return tokenA.Cmp(tokenB)
}

// https://github.com/apache/cassandra/blob/06b2bd3b886041b9e7904138295532939a67540f/src/java/org/apache/cassandra/utils/FBUtilities.java#L245-L248
func keyToToken(key []byte) *big.Int {
	tokenBytes := md5.Sum(key)
	token := new(big.Int).SetBytes(tokenBytes[:])

	// two's complement
	if len(tokenBytes) > 0 && tokenBytes[0]&0x80 > 0 {
		one := big.NewInt(1)
		token.Sub(token, one.Lsh(one, uint(len(tokenBytes))*8))

		// cassandra takes the Abs of the md5 value
		token.Abs(token)
	}

	return token
}
