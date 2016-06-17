package main

import (
	"fmt"
	"io/ioutil"
	"os"
	"testing"

	"github.com/Financial-Times/up-rw-app-api-go/rwapi"
	"github.com/stretchr/testify/assert"
)

func TestBoltReadWrite(t *testing.T) {
	testWithBolt(t, testReadWrite)
}

func TestBoltReadDelete(t *testing.T) {
	testWithBolt(t, testDelete)
}

func TestBoltIDs(t *testing.T) {
	testWithBolt(t, testIDs)
}
func TestBoltCount(t *testing.T) {
	testWithBolt(t, testCount)
}

func testWithBolt(t *testing.T, f func(t *testing.T, e Engine)) {
	testDir, err := ioutil.TempDir(os.TempDir(), "restorage-bolt-test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(testDir)

	be, err := NewBoltEngine(testDir, "coll1", "id", true)
	if err != nil {
		t.Fatal(err)
	}
	defer be.Close()

	f(t, be)
}

func testReadWrite(t *testing.T, e Engine) {
	assert := assert.New(t)

	res := Document{
		"id":   "1",
		"name": "foo",
	}

	err := e.Write(res)
	if err != nil {
		t.Error(err)
	}

	doc, found, err := e.Read("1")
	assert.NoError(err)
	assert.True(found)

	assert.Equal(res, doc)

}

func testDelete(t *testing.T, e Engine) {
	assert := assert.New(t)

	res := Document{
		"id":   "2",
		"name": "foo",
	}

	doc, found, err := e.Read("2")
	assert.NoError(err)
	assert.False(found)
	assert.Nil(doc)

	deleted, err := e.Delete("2")
	assert.False(deleted)

	err = e.Write(res)
	assert.NoError(err)

	doc, found, err = e.Read("2")
	assert.NoError(err)
	assert.True(found)
	assert.Equal(res, doc)

	deleted, err = e.Delete("2")
	assert.True(deleted)
	assert.NoError(err)

	doc, found, err = e.Read("2")
	assert.NoError(err)
	assert.False(found)
	assert.Nil(doc)
}

func testIDs(t *testing.T, e Engine) {
	assert := assert.New(t)

	resources := map[string]Document{
		"1": Document{
			"id":   "1",
			"name": "foo",
		}, "2": Document{
			"id":   "2",
			"name": "bar",
		}, "3": Document{
			"id":   "3",
			"name": "baz",
		},
	}

	for _, r := range resources {
		assert.NoError(e.Write(r))
	}

	f := func(id rwapi.IDEntry) (bool, error) {
		_, exists := resources[id.ID]
		assert.True(exists)
		delete(resources, id.ID)
		return true, nil
	}

	assert.NoError(e.IDs(f))

	assert.Equal(0, len(resources))

}

func testCount(t *testing.T, e Engine) {
	assert := assert.New(t)

	resources := make([]Document, 7)
	for i := range resources {
		resources[i] = Document{
			"id":   fmt.Sprintf("%d", i),
			"name": fmt.Sprintf("foo %d", i),
		}
	}

	for _, r := range resources {
		assert.NoError(e.Write(r))
	}

	count, err := e.Count()
	assert.NoError(err)
	assert.Equal(len(resources), count)

}
