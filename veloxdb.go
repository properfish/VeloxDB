package velox

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"sync"

	jsoniter "github.com/json-iterator/go"
	cmap "github.com/orcaman/concurrent-map"
)

type Record struct {
	ID   int         `json:"id"`
	Data interface{} `json:"data"`
}

type Table struct {
	records cmap.ConcurrentMap
	nextID  int
	mutex   sync.RWMutex
}

type Database struct {
	tables cmap.ConcurrentMap
	folder string
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
		records: cmap.New(),
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

func (table *Table) AddRecord(record interface{}) (int, error) {
	table.mutex.Lock()
	defer table.mutex.Unlock()

	id := table.nextID
	table.nextID++

	data := Record{
		ID:   id,
		Data: record,
	}

	table.records.Set(strconv.Itoa(id), data)
	return id, nil
}

func (table *Table) GetRecord(id int) (interface{}, error) {
	val, ok := table.records.Get(strconv.Itoa(id))
	if !ok {
		return nil, errors.New("GetRecord: record not found")
	}

	record, ok := val.(Record)
	if !ok {
		return nil, errors.New("invalid record type")
	}

	return record.Data, nil
}

func (table *Table) UpdateRecord(id int, record interface{}) error {
	table.mutex.Lock()
	defer table.mutex.Unlock()

	val, ok := table.records.Get(strconv.Itoa(id))
	if !ok {
		return errors.New("UpdateRecord: record not found")
	}

	updateRecord, ok := val.(Record)
	if !ok {
		return errors.New("invalid record type")
	}

	updateRecord.Data = record
	table.records.Set(strconv.Itoa(id), updateRecord)

	return nil
}

func (table *Table) DeleteRecord(id int) error {
	table.mutex.Lock()
	defer table.mutex.Unlock()

	_, ok := table.records.Get(strconv.Itoa(id))
	if !ok {
		return errors.New("DeleteRecord: record not found")
	}

	table.records.Remove(strconv.Itoa(id))
	return nil
}

func (database *Database) Load(folder string) error {
	file, err := os.Open(folder + "master.json")
	if err != nil {
		return fmt.Errorf("Database_Load: %s", err)
	}
	database.folder = folder
	defer file.Close()

	var tables map[string][]Record
	if err := jsoniter.NewDecoder(file).Decode(&tables); err != nil {
		return fmt.Errorf("Database_Load: %s", err)
	}

	for name, records := range tables {
		table := NewTable()

		for _, record := range records {
			table.records.Set(strconv.Itoa(record.ID), record)
			if record.ID >= table.nextID {
				table.nextID = record.ID + 1
			}
		}

		database.tables.Set(name, table)
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

	return nil
}
