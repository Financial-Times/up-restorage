package main

import (
	"errors"
	"labix.org/v2/mgo"
	"labix.org/v2/mgo/bson"
	"log"
)

type mongoEngine struct {
	session *mgo.Session
	dbName  string
}

func (eng mongoEngine) Close() {
	// TODO
}

// NewMongoEngine returns an Engine based on a mongodb batabase backend
func NewMongoEngine(dbName string, hostPorts string) Engine {
	log.Printf("connecting to mongodb '%s'\n", hostPorts)
	s, err := mgo.Dial(hostPorts)
	if err != nil {
		panic(err)
	}
	s.SetMode(mgo.Monotonic, true)
	eng := &mongoEngine{
		session: s,
		dbName:  dbName,
	}

	eng.EnsureIndexes("organisations") //TODOL parameterise this

	return eng
}

func (eng *mongoEngine) EnsureIndexes(collection string) {
	c := eng.session.DB(eng.dbName).C(collection)

	eng.session.ResetIndexCache()

	// create collection if it's not there
	c.Create(&mgo.CollectionInfo{})

	err := c.EnsureIndex(mgo.Index{
		Key:        []string{"uuid"}, //TODO: parameterise this
		Unique:     true,
		DropDups:   true,
		Background: false,
		Sparse:     false,
	})
	if err != nil {
		panic(err)
	}

}

func (eng *mongoEngine) Drop(collection string) {
	err := eng.session.DB(eng.dbName).C(collection).DropCollection()
	if err != nil {
		log.Printf("failed to drop collection")
	}
	eng.EnsureIndexes(collection)
}

func (eng *mongoEngine) Write(collection string, id string, cont Document) error {
	coll := eng.session.DB(eng.dbName).C(collection)
	if id == "" {
		return errors.New("missing id")
	}
	_, err := coll.Upsert(bson.D{{"uuid", id}}, cont) //TODO: parameterise uuid
	if err != nil {
		log.Printf("insert failed: %v\n", err)
	}
	return nil
}

func (eng *mongoEngine) Count(collection string) int {
	coll := eng.session.DB(eng.dbName).C(collection)
	count, err := coll.Count()
	if err != nil {
		panic(err)
	}
	return count
}

func (eng *mongoEngine) Load(collection, id string) (bool, Document, error) {
	c := eng.session.DB(eng.dbName).C(collection)
	var content Document
	err := c.Find(bson.M{"uuid": id}).One(&content)
	if err == mgo.ErrNotFound {
		return false, Document{}, nil
	}
	if err != nil {
		return false, Document{}, err
	}
	return true, content, nil
}

func (eng mongoEngine) All(collection string, stopchan chan struct{}) (chan Document, error) {
	cont := make(chan Document)

	go func() {
		defer close(cont)
		coll := eng.session.DB(eng.dbName).C(collection)
		iter := coll.Find(nil).Iter()
		var result Document
		for iter.Next(&result) {
			select {
			case <-stopchan:
				break
			case cont <- result:
			}
		}
		if err := iter.Close(); err != nil {
			panic(err)
		}
	}()

	return cont, nil
}
