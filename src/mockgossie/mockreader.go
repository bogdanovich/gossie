package mockgossie

import (
	"bytes"

	. "github.com/betable/gossie/src/cassandra"
	. "github.com/betable/gossie/src/gossie"
)

type MockReader struct {
	pool        *MockConnectionPool
	columnLimit int
	rowLimit    int
	cf          string
	slice       *Slice

	startKey []byte
}

var _ Reader = &MockReader{}

func (m *MockReader) ConsistencyLevel(ConsistencyLevel) Reader              { return m }
func (m *MockReader) Columns([][]byte) Reader                               { panic("not implemented") }
func (m *MockReader) Where(column []byte, op Operator, value []byte) Reader { panic("not implemented") }
func (m *MockReader) IndexedGet(*IndexedRange) ([]*Row, error)              { panic("not implemented") }
func (m *MockReader) SetTokenRangeCount(count int) Reader                   { return m }
func (m *MockReader) WideRowScan(key, startColumn []byte, batchSize int32, callback func(*Column) bool) error {
	panic("not implemented")
}

func newReader(m *MockConnectionPool) *MockReader {
	return &MockReader{
		pool:        m,
		rowLimit:    DEFAULT_ROW_LIMIT,
		columnLimit: DEFAULT_COLUMN_LIMIT,
	}
}

func (m *MockReader) Cf(cf string) Reader {
	m.cf = cf
	return m
}

func (m *MockReader) Slice(s *Slice) Reader {
	m.slice = s
	return m
}

func (m *MockReader) SetTokenRange(startToken, endToken string) Reader {
	// For now, only allow a token range that is equivalent to the whole range
	if startToken == "-1" && endToken == "170141183460469231731687303715884105728" {
		return m
	}
	panic("not implemented")
}

func (m *MockReader) SetKeyRange(startKey, endKey []byte) Reader {
	// For now, only allow `(startKey, nil)` as a common case (resume scan using last key)
	if startKey != nil && endKey == nil {
		m.startKey = startKey
		return m
	}
	panic("not implemented")
}

func (m *MockReader) Get(key []byte) (*Row, error) {
	rows := m.pool.Rows(m.cf)

	for _, r := range rows {
		if bytes.Equal(r.Key, key) {
			checkExpired(r)
			return m.sliceRow(r), nil
		}
	}
	return nil, nil
}

func (m *MockReader) Count(key []byte) (int, error) {
	r, err := m.Get(key)
	if err != nil {
		return 0, err
	}

	return len(r.Columns), nil
}

func (m *MockReader) MultiCount(keys [][]byte) ([]*RowColumnCount, error) {
	counts := make([]*RowColumnCount, len(keys))

	for i, key := range keys {
		r, err := m.Get(key)
		if err != nil {
			return nil, err
		}
		counts[i] = &RowColumnCount{
			Key:   key,
			Count: len(r.Columns),
		}
	}

	return counts, nil
}

func (m *MockReader) RangeGet(r *Range) ([]*Row, error) {
	rows := m.pool.Rows(m.cf)
	count := r.Count
	started := false
	keys := make([][]byte, 0)
	for _, row := range rows {
		if started || r.Start == nil || bytes.Equal(row.Key, r.Start) {
			started = true
			keys = append(keys, row.Key)
			count--
		}
		if count <= 0 || (r.End != nil && bytes.Equal(row.Key, r.End)) {
			break
		}
	}
	return m.MultiGet(keys)
}

func (m *MockReader) MultiGet(keys [][]byte) ([]*Row, error) {
	rows := m.pool.Rows(m.cf)

	buffer := make([]*Row, 0)
	for _, key := range keys {
		for _, r := range rows {
			if bytes.Equal(r.Key, key) {
				checkExpired(r)
				buffer = append(buffer, m.sliceRow(r))
			}
		}
	}

	return rows, nil
}

func (m *MockReader) RangeScan() (<-chan *Row, <-chan error) {
	data := make(chan *Row)
	errc := make(chan error)

	startFound := m.startKey == nil

	go func() {
		defer close(data)
		defer close(errc)

		rows := m.pool.Rows(m.cf)
		for _, row := range rows {
			checkExpired(row)
			if !startFound {
				// If a startKey has been set, don't return results until we
				// reach that portion of the range (inclusive)
				if compareRowKeys(row.Key, m.startKey) >= 0 {
					startFound = true
				} else {
					continue
				}
			}

			data <- m.sliceRow(row)
		}
	}()

	return data, errc
}

func (m *MockReader) sliceRow(r *Row) *Row {
	if m.slice != nil {
		slice := m.slice
		if slice.Reversed {
			slice.Start, slice.End = slice.End, slice.Start
		}
		cr := *r
		cr.Columns = []*Column{}
		for _, c := range r.Columns {
			if len(slice.Start) > 0 && bytes.Compare(slice.Start, c.Name) > 0 {
				continue
			}
			if len(slice.End) > 0 && bytes.Compare(slice.End, c.Name) < 0 {
				continue
			}
			cr.Columns = append(cr.Columns, c)
		}
		if len(cr.Columns) > slice.Count {
			if slice.Reversed {
				cr.Columns = cr.Columns[(len(cr.Columns) - slice.Count):len(cr.Columns)]
			} else {
				cr.Columns = cr.Columns[0:slice.Count]
			}
		}
		r = &cr
	}
	return r
}
