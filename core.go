package main

import (
	"errors"

	"github.com/Financial-Times/up-rw-app-api-go/rwapi"
)

type Engine interface {
	rwapi.Service
	rwapi.IDService

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
