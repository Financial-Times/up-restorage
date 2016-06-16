package main

import (
	"errors"
)

type Engine interface {
	Write(c CollectionSettings, id string, doc Document) error
	Read(c CollectionSettings, id string) (bool, Document, error)
	Delete(c CollectionSettings, id string) error
	Count(c CollectionSettings) (int, error)
	All(c CollectionSettings, stopchan chan struct{}) (chan Document, error)
	Ids(c CollectionSettings, stopchan chan struct{}) (chan string, error)
	Drop(c CollectionSettings) (bool, error)
	Close()
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
