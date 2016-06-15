package main

import (
	"errors"
)

type Engine interface {
	Load(c Collection, id string) (bool, Document, error)
	Write(c Collection, id string, doc Document) error
	Count(c Collection) int
	All(c Collection, stopchan chan struct{}) (chan Document, error)
	Ids(c Collection, stopchan chan struct{}) (chan string, error)
	Delete(c Collection, id string) error
	Drop(c Collection) (bool, error)
	Close()
}

var (
	ErrInvalidQuery = errors.New("invalid query")
	ErrNotFound     = errors.New("Not found")
)

type Document map[string]interface{}

type Collections map[string]Collection

type Collection struct {
	name           string
	idPropertyName string
}
