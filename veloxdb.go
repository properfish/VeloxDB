package velox

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"sync"
	"time"

	jsoniter "github.com/json-iterator/go"
	cmap "github.com/orcaman/concurrent-map"
)

type Record struct {
	ID   int         `json:"id"`
	Data interface{} `json:"data"`
}

type RecordInterface interface {
	GetID() int
	GetData() interface{}
}

func (record *Record) GetID() int {
	return record.ID
}

func (record *Record) GetData() interface{} {
	return record.Data
}

type Table struct {
	records *cmap.ConcurrentMap
	nextID  int
	sync.RWMutex
}

type TableInterface interface {
	CreateRecord(record interface{}) (RecordInterface, error)
	ReadRecord(id int) (RecordInterface, error)
	UpdateRecord(id int, record interface{}) error
	DeleteRecord(id int) error
}

func (table *Table) CreateRecord(record interface{}) (RecordInterface, error) {
	table.RWMutex.Lock()
	defer table.RWMutex.Unlock()

	id := table.nextID
	table.nextID++

	data := Record{
		ID:   id,
		Data: record,
	}

	table.records.Set(strconv.Itoa(id), data)
	return &data, nil
}

func (table *Table) ReadRecord(id int) (interface{}, error) {
	val, ok := table.records.Get(strconv.Itoa(id))
	if !ok {
		return nil, errors.New("ReadRecord: record not found")
	}

	record, ok := val.(Record)
	if !ok {
		return nil, errors.New("ReadRecord: invalid record type")
	}

	return record.Data, nil
}

func (t *Table) UpdateRecord(id int, record interface{}) error {
	t.RWMutex.Lock()
	defer t.RWMutex.Unlock()

	val, ok := t.records.Get(strconv.Itoa(id))
	if !ok {
		return errors.New("UpdateRecord: record not found")
	}

	updateRecord, ok := val.(Record)
	if !ok {
		return errors.New("UpdateRecord: invalid record type")
	}

	updateRecord.Data = record
	t.records.Set(strconv.Itoa(id), updateRecord)

	return nil
}

func (t *Table) DeleteRecord(id int) error {
	t.RWMutex.Lock()
	defer t.RWMutex.Unlock()

	_, ok := t.records.Get(strconv.Itoa(id))
	if !ok {
		return errors.New("DeleteRecord: record not found")
	}

	t.records.Remove(strconv.Itoa(id))
	return nil
}

type Database struct {
	tables   cmap.ConcurrentMap
	folder   string
	lastSave string
}

func NewDatabase() *Database {
	return &Database{
		tables: cmap.New(),
	}
}

func (database *Database) CreateTable(name string) error {
	if _, ok := database.tables.Get(name); ok {
		return errors.New("CreateTable: table already exists")
	}

	database.tables.Set(name, *NewTable())
	return nil
}

func NewTable() *Table {
	return &Table{
		records: &cmap.ConcurrentMap{},
		nextID:  1,
	}
}

func (database *Database) GetTable(name string) (interface{}, error) {
	table, ok := database.tables.Get(name)
	if !ok {
		return nil, errors.New("GetTable: table not found")
	}

	return &table, nil
}

func (database *Database) Load(folder string) error {
	file, err := os.Open(folder + "master.json")
	if err != nil {
		return fmt.Errorf("Database_Load: %s", err)
	}
	database.folder = folder
	defer file.Close()

	var tables map[string]interface{}
	if err := jsoniter.NewDecoder(file).Decode(&tables); err != nil {
		return fmt.Errorf("Database_Load: %s", err)
	}

	for name := range tables {

		table := NewTable()

		tbl, err := os.Open(fmt.Sprintf("%s%s.json", folder, name))
		if err != nil {
			return fmt.Errorf("Database_Load: %s", err)
		}

		var records []Record
		if err := jsoniter.NewDecoder(tbl).Decode(&records); err != nil {

			for _, record := range records {
				table.records.Set(strconv.Itoa(record.ID), record)
				if record.ID >= table.nextID {
					table.nextID = record.ID + 1
				}
			}

			database.tables.Set(name, table)
		}

	}
	return nil
}

func (database *Database) Save() error {
	database.tables.IterCb(func(key string, val interface{}) {
		table := val.(*Table)

		filename := fmt.Sprintf("%s%s%s.json", database.folder, string(os.PathSeparator), key)

		data := make([]Record, 0)

		table.records.IterCb(func(key string, val interface{}) {
			data = append(data, val.(Record))
		})

		encoded, err := jsoniter.Marshal(data)
		if err != nil {
			fmt.Printf("Database_Save: Error marshaling data for table %s: %v\n", key, err)
			return
		}

		if err := os.WriteFile(filename, encoded, 0644); err != nil {
			fmt.Printf("Database_Save: Error writing data for table %s: %v\n", key, err)
		}
	})
	database.lastSave = time.Now().String()
	return nil
}
