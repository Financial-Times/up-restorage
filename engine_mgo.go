package main

import (
	"errors"
	"log"

	"fmt"

	"github.com/kr/pretty"
	"github.com/pborman/uuid"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

type mongoEngine struct {
	session        *mgo.Session
	dbName         string
	collectionName string
	idPropertyName string
	isBinaryId     bool
}

func (eng mongoEngine) Close() {
	// TODO
}

// NewMongoEngine returns an Engine based on a mongodb database backend
func NewMongoEngine(dbName string, collectionName string, idPropertyName string, isBinaryId bool, s *mgo.Session) Engine {
	eng := &mongoEngine{
		session:        s,
		dbName:         dbName,
		collectionName: collectionName,
		idPropertyName: idPropertyName,
		isBinaryId:     isBinaryId,
	}

	eng.EnsureIndexes()

	return eng
}

func (eng *mongoEngine) EnsureIndexes() {
	c := eng.session.DB(eng.dbName).C(eng.collectionName)

	eng.session.ResetIndexCache()

	// create collection if it's not there
	c.Create(&mgo.CollectionInfo{})

	err := c.EnsureIndex(mgo.Index{
		Key:        []string{eng.idPropertyName},
		Unique:     true,
		DropDups:   true,
		Background: false,
		Sparse:     false,
	})
	if err != nil {
		log.Printf("failed to create index by %s on %s : %v\n", eng.collectionName, eng.idPropertyName, err.Error())
	}

}

func (eng *mongoEngine) Drop() (bool, error) {
	err := eng.session.DB(eng.dbName).C(eng.collectionName).DropCollection()
	if err != nil {
		log.Printf("failed to drop collection")
	}
	eng.EnsureIndexes()
	//TODO implement error handling and whether drop is successful or not
	return true, nil
}

func (eng *mongoEngine) Write(cont Document) error {
	id, ok := cont[eng.idPropertyName].(string)
	if !ok || id == "" {
		return errors.New("missing or invalid id")
	}
	coll := eng.session.DB(eng.dbName).C(eng.collectionName)
	if id == "" {
		return errors.New("missing id")
	}
	_, err := coll.Upsert(bson.D{{eng.idPropertyName, id}}, cont)
	if err != nil {
		log.Printf("insert failed: %v\n", err)
	}
	return nil
}

func (eng *mongoEngine) Count() (int, error) {
	return eng.session.DB(eng.dbName).C(eng.collectionName).Count()
}

func (eng *mongoEngine) Read(id string) (bool, Document, error) {
	c := eng.session.DB(eng.dbName).C(eng.collectionName)
	var content Document

	var err error
	if eng.isBinaryId {
		binaryId := bson.Binary{Kind: 0x04, Data: []byte(uuid.Parse(id))}
		err = c.Find(bson.M{eng.idPropertyName: binaryId}).One(&content)
	} else {
		err = c.Find(bson.M{eng.idPropertyName: id}).One(&content)
	}

	if err == mgo.ErrNotFound {
		return false, Document{}, nil
	}
	if err != nil {
		return false, Document{}, err
	}
	cleanup(content)
	if eng.isBinaryId {
		content[eng.idPropertyName] = id
	}
	return true, content, nil
}

func (eng *mongoEngine) Delete(id string) (bool, error) {
	c := eng.session.DB(eng.dbName).C(eng.collectionName)
	err := c.Remove(bson.M{eng.idPropertyName: id})
	if err != nil {
		if err == mgo.ErrNotFound {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func (eng mongoEngine) All(stopchan chan struct{}) (chan Document, error) {
	cont := make(chan Document)

	go func() {
		defer close(cont)
		coll := eng.session.DB(eng.dbName).C(eng.collectionName)
		iter := coll.Find(nil).Iter()
		result := &Document{}
		for iter.Next(result) {
			cleanup(*result)
			select {
			case <-stopchan:
				break
			case cont <- *result:
				result = &Document{}
			}
		}
		if err := iter.Close(); err != nil {
			panic(err)
		}
	}()

	return cont, nil
}

func (eng mongoEngine) Ids(stopchan chan struct{}) (chan string, error) {
	ids := make(chan string)
	go func() {
		defer close(ids)
		coll := eng.session.DB(eng.dbName).C(eng.collectionName)
		iter := coll.Find(nil).Select(bson.M{eng.idPropertyName: true}).Iter()
		var result map[string]interface{}
		for iter.Next(&result) {
			select {
			case <-stopchan:
				break
			case ids <- getUUIDString(result[eng.idPropertyName]):
			}
		}
		if err := iter.Close(); err != nil {
			panic(err)
		}
	}()
	return ids, nil
}

func (ee mongoEngine) IDPropertyName() string {
	return ee.idPropertyName
}

func getUUIDString(uuidValue interface{}) string {
	if uuidString, ok := uuidValue.(string); ok {
		return uuidString
	} else if binaryId, ok := uuidValue.(bson.Binary); ok {
		return uuid.UUID(binaryId.Data).String()
	} else {
		fmt.Printf("UUID field is in an unknown format!\n %# v", pretty.Formatter(uuidValue))
		return ""
	}
}

func cleanup(doc Document) {
	delete(doc, "_id")
}
