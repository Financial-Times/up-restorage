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

// NewMongoEngine returns an Engine based on a mongodb database backend
func NewMongoEngine(dbName string, collections Collections, hostPorts string) Engine {
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

	for _, coll := range collections {
		eng.EnsureIndexes(coll)
	}

	return eng
}

func (eng *mongoEngine) EnsureIndexes(collection Collection) {
	c := eng.session.DB(eng.dbName).C(collection.name)

	eng.session.ResetIndexCache()

	// create collection if it's not there
	c.Create(&mgo.CollectionInfo{})

	err := c.EnsureIndex(mgo.Index{
		Key:        []string{collection.idPropertyName},
		Unique:     true,
		DropDups:   true,
		Background: false,
		Sparse:     false,
	})
	if err != nil {
		panic(err)
	}

}

func (eng *mongoEngine) Drop(collection Collection) {
	err := eng.session.DB(eng.dbName).C(collection.name).DropCollection()
	if err != nil {
		log.Printf("failed to drop collection")
	}
	eng.EnsureIndexes(collection)
}

func (eng *mongoEngine) Write(collection Collection, id string, cont Document) error {
	coll := eng.session.DB(eng.dbName).C(collection.name)
	if id == "" {
		return errors.New("missing id")
	}
	_, err := coll.Upsert(bson.D{{collection.idPropertyName, id}}, cont)
	if err != nil {
		log.Printf("insert failed: %v\n", err)
	}
	return nil
}

func (eng *mongoEngine) Count(collection Collection) int {
	coll := eng.session.DB(eng.dbName).C(collection.name)
	count, err := coll.Count()
	if err != nil {
		panic(err)
	}
	return count
}

func (eng *mongoEngine) Load(collection Collection, id string) (bool, Document, error) {
	c := eng.session.DB(eng.dbName).C(collection.name)
	var content Document
	err := c.Find(bson.M{collection.idPropertyName: id}).One(&content)
	if err == mgo.ErrNotFound {
		return false, Document{}, nil
	}
	if err != nil {
		return false, Document{}, err
	}
	cleanup(content)
	return true, content, nil
}

func (eng mongoEngine) All(collection Collection, stopchan chan struct{}) (chan Document, error) {
	cont := make(chan Document)

	go func() {
		defer close(cont)
		coll := eng.session.DB(eng.dbName).C(collection.name)
		iter := coll.Find(nil).Iter()
		var result Document
		for iter.Next(&result) {
			cleanup(result)
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

func cleanup(doc Document) {
	delete(doc, "_id")
}
