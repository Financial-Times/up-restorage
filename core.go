package main

import (
	"errors"
)

type Engine interface {
	Load(collection, id string) (bool, Document, error)
	Write(collection, id string, c Document) error
	Count(collection string) int
	All(collection string, stopchan chan struct{}) (chan Document, error)
	Drop(collection string)
	Close()
}

var (
	ERR_NOT_IMPLEMENTED = errors.New("not implemented")
	ERR_INVALID_QUERY   = errors.New("invalid query")
)

type Document map[string]interface{}
