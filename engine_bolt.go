package main

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"time"

	"github.com/Financial-Times/up-rw-app-api-go/rwapi"
	"github.com/boltdb/bolt"
)

type boltEngine struct {
	db             *bolt.DB
	collectionName []byte
	idPropertyName string
}

func init() {
	gob.Register([]interface{}{})
	gob.Register(map[string]interface{}{})
}

func NewBoltEngine(datadir string, collectionName string, idPropertyName string, unsafe bool) (Engine, error) {

	if err := os.MkdirAll(datadir, 0700); err != nil {
		return nil, err
	}

	db, err := bolt.Open(filepath.Join(datadir, collectionName), 0600, &bolt.Options{Timeout: 1 * time.Second})
	if err != nil {
		return nil, err
	}

	db.NoSync = unsafe

	if err := db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte(collectionName))
		return err
	}); err != nil {
		return nil, err
	}

	e := &boltEngine{
		db, []byte(collectionName), idPropertyName,
	}

	return e, nil
}

func (ee *boltEngine) Drop() (bool, error) {
	err := ee.db.Update(func(tx *bolt.Tx) error {
		return tx.DeleteBucket(ee.collectionName)
	})
	return true, err // FIXME:
}

func (ee *boltEngine) Write(resource interface{}) error {
	doc := resource.(Document)
	id, err := ee.getID(doc)
	if err != nil {
		return err
	}
	return ee.db.Batch(func(tx *bolt.Tx) error {
		return tx.Bucket(ee.collectionName).Put(id, ee.ser(doc))
	})
}

func (ee *boltEngine) deser(data []byte) (Document, error) {
	var doc Document
	dec := gob.NewDecoder(bytes.NewReader(data))
	err := dec.Decode(&doc)
	if err != nil {
		return nil, err
	}
	return doc, nil
}

func (ee *boltEngine) ser(doc Document) []byte {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	err := enc.Encode(doc)
	if err != nil {
		panic(err)
	}
	bytes := buf.Bytes()

	return bytes
}

func (ee *boltEngine) Delete(id string) (bool, error) {
	var found bool
	err := ee.db.Update(func(tx *bolt.Tx) error {
		id := []byte(id)
		current := tx.Bucket(ee.collectionName).Get(id)
		if current == nil {
			found = false
			return nil
		}
		found = true
		return tx.Bucket(ee.collectionName).Delete(id)
	})
	return found, err
}

func (ee *boltEngine) Count() (int, error) {
	count := 0
	err := ee.db.View(func(tx *bolt.Tx) error {
		count = tx.Bucket(ee.collectionName).Stats().KeyN
		return nil
	})
	return count, err
}

func (ee *boltEngine) Read(id string) (interface{}, bool, error) {
	var result []byte
	err := ee.db.View(func(tx *bolt.Tx) error {
		result = tx.Bucket(ee.collectionName).Get([]byte(id))
		return nil
	})
	if result == nil {
		return nil, false, err
	}
	doc, err := ee.deser(result)
	if err != nil {
		return nil, false, err
	}
	return doc, true, nil

}

func (ee boltEngine) IDs(f func(rwapi.IDEntry) (bool, error)) error {
	return ee.db.View(func(tx *bolt.Tx) error {
		return tx.Bucket(ee.collectionName).ForEach(func(k []byte, v []byte) error {
			more, err := f(rwapi.IDEntry{ID: string(k)})
			if err != nil {
				return err
			}
			if !more {
				return errors.New("aborted")
			}
			return nil
		})
	})
}

func (ee boltEngine) IDPropertyName() string {
	return ee.idPropertyName
}

func (ee boltEngine) getID(doc Document) ([]byte, error) {
	if id, ok := doc[ee.idPropertyName].(string); ok {
		return []byte(id), nil
	}
	return nil, errors.New("no id found in document")
}

func (ee boltEngine) Close() {}

func (ee boltEngine) Initialise() error {
	return nil
}

func (ee boltEngine) Check() error {
	return nil
}

func (ee boltEngine) DecodeJSON(dec *json.Decoder) (interface{}, string, error) {
	var doc Document
	if err := dec.Decode(&doc); err != nil {
		return nil, "", err
	}

	id, ok := doc[ee.idPropertyName].(string)
	if !ok {
		return nil, "", errors.New("no id found in document")
	}

	return doc, id, nil
}
