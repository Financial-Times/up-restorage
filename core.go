package main

import (
	"errors"
)

type Engine interface {
	Write(doc Document) error
	Read(id string) (bool, Document, error)
	Delete(id string) error
	Count() (int, error)
	All(stopchan chan struct{}) (chan Document, error)
	Ids(stopchan chan struct{}) (chan string, error)
	Drop() (bool, error)
	Close()
	IDPropertyName() string
}

var (
	ErrInvalidQuery = errors.New("invalid query")
	ErrNotFound     = errors.New("Not found")
)

type Document map[string]interface{}

type CollectionSettings struct {
	name           string
	idPropertyName string
}
