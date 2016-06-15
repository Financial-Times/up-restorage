package main

import (
	"errors"
)

type Engine interface {
	Write(c Collection, id string, doc Document) error
	Read(c Collection, id string) (bool, Document, error)
	Delete(c Collection, id string) error
	Count(c Collection) (int, error)
	All(c Collection, stopchan chan struct{}) (chan Document, error)
	Ids(c Collection, stopchan chan struct{}) (chan string, error)
	Drop(c Collection) (bool, error)
	Close()
}

var (
	ErrInvalidQuery = errors.New("invalid query")
	ErrNotFound     = errors.New("Not found")
)

type Document map[string]interface{}

type Collection struct {
	name           string
	idPropertyName string
}
