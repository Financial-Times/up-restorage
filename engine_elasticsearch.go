package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"strings"

	"github.com/Financial-Times/up-rw-app-api-go/rwapi"
)

type elasticEngine struct {
	client         *http.Client
	baseURL        string
	indexName      string
	collectionName string
	idPropertyName string
}

func NewElasticEngine(elasticURL string, indexName string, collectionName string, idPropertyName string, client *http.Client) Engine {
	e := &elasticEngine{
		client:         client,
		baseURL:        elasticURL,
		indexName:      indexName,
		collectionName: collectionName,
		idPropertyName: idPropertyName,
	}
	for strings.HasSuffix(e.baseURL, "/") {
		e.baseURL = e.baseURL[0 : len(e.baseURL)-1]
	}

	return e
}

func (ee *elasticEngine) Drop() (bool, error) {
	req, err := http.NewRequest("DELETE", fmt.Sprintf("%s/%s?_type=%s", ee.baseURL, ee.indexName, ee.collectionName), nil)
	if err != nil {
		return false, err
	}
	resp, err := ee.client.Do(req)
	if err != nil {
		return false, err
	}
	switch {
	case resp.StatusCode == 200:
		return true, nil

	case resp.StatusCode == 404:
		return false, nil

	default:
		return false, fmt.Errorf("drop fail : %s", resp.Status)
	}
}

func (ee *elasticEngine) Write(resource interface{}) error {
	cont := resource.(Document)
	id, ok := cont[ee.idPropertyName].(string)
	if !ok || id == "" {
		return errors.New("missing or invalid id")
	}

	doneWrite := make(chan struct{})
	writeErr := make(chan error, 1)

	r, w := io.Pipe()
	go func() {
		je := json.NewEncoder(w)
		err := je.Encode(cont)
		if err != nil {
			writeErr <- err
		}
		w.Close()
		close(doneWrite)
	}()

	u := fmt.Sprintf("%s/%s/%s/%s", ee.baseURL, ee.indexName, ee.collectionName, id)
	req, err := http.NewRequest("PUT", u, r)
	if err != nil {
		return err
	}
	resp, err := ee.client.Do(req)
	if err != nil {
		return err
	}
	defer func() {
		io.Copy(ioutil.Discard, resp.Body)
		resp.Body.Close()
	}()

	select {
	case e := <-writeErr:
		return e
	case <-doneWrite:
		switch resp.StatusCode {
		case http.StatusOK:
			return nil
		case http.StatusCreated:
			return nil
		default:
			return fmt.Errorf("error in ES request : %s\n", resp.Status)
		}
	}
}

func (ee *elasticEngine) Delete(id string) (bool, error) {
	if id == "" {
		return false, errors.New("missing id")
	}

	req, err := http.NewRequest("DELETE", fmt.Sprintf("%s/%s/%s/%s", ee.baseURL, ee.indexName, ee.collectionName, id), nil)
	if err != nil {
		return false, err
	}
	resp, err := ee.client.Do(req)
	if err != nil {
		fmt.Printf("e: %v\n", err)
		return false, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 400 && resp.StatusCode != 404 {
		return false, fmt.Errorf("delete request failed with status %d", resp.Status)
	}

	deleted := resp.StatusCode != 404
	return deleted, nil
}

func (ee *elasticEngine) Count() (int, error) {
	res, err := ee.client.Get(fmt.Sprintf("%s/%s/%s/_count", ee.baseURL, ee.indexName, ee.collectionName))
	if err != nil {
		return 0, err
	}
	defer res.Body.Close()
	if res.StatusCode == 200 {
		dec := json.NewDecoder(res.Body)
		var result esCountResult
		err := dec.Decode(&result)
		if err != nil {
			return 0, err
		}
		return result.Count, nil
	}
	if res.StatusCode == 404 {
		return 0, nil
	}
	return 0, fmt.Errorf("count failed : %s", res.Status)
}

type esCountResult struct {
	Count int `json:"count"`
}

func (ee *elasticEngine) Read(id string) (interface{}, bool, error) {
	res, err := ee.client.Get(fmt.Sprintf("%s/%s/%s/%s", ee.baseURL, ee.indexName, ee.collectionName, id))
	if err != nil {
		return nil, false, err
	}
	defer res.Body.Close()
	switch {
	case res.StatusCode == 200:
		dec := json.NewDecoder(res.Body)
		var result esGetResult
		err := dec.Decode(&result)
		if err != nil {
			return nil, false, err
		}
		return result.Source, true, nil
	case res.StatusCode == 404:
		return nil, false, nil
	default:
		return nil, false, fmt.Errorf("read fail: %s", res.Status)
	}
}

type esGetResult struct {
	Source Document `json:"_source"`
}

func (ee elasticEngine) IDs(callback func(rwapi.IDEntry) (bool, error)) error {
	count, err := ee.Count()
	if err != nil {
		return err
	}

	q := fmt.Sprintf("{\"query\":{\"match_all\": {}}, \"fields\":[], \"size\": %d,  \"from\": 0}", count+1000)
	res, err := ee.client.Post(fmt.Sprintf("%s/%s/%s/_search", ee.baseURL, ee.indexName, ee.collectionName), "application/json", strings.NewReader(q))
	if err != nil {
		return err
	}

	switch {
	case res.StatusCode == 200:
	case res.StatusCode == 400:
		res.Body.Close()
		return ErrInvalidQuery
	case res.StatusCode == 404:
		return ErrNotFound
	default:
		return fmt.Errorf("query failed: %s", res.Status)
	}

	defer res.Body.Close()

	var result esSearchResult
	if err := json.NewDecoder(res.Body).Decode(&result); err != nil {
		return err
	}
	for _, h := range result.Hits.Hits {
		more, err := callback(rwapi.IDEntry{ID: h.ID})
		if !more || err != nil {
			return err
		}
	}

	return nil
}

func (ee elasticEngine) IDPropertyName() string {
	return ee.idPropertyName
}

func (ee elasticEngine) DecodeJSON(dec *json.Decoder) (interface{}, string, error) {
	var doc Document
	if err := dec.Decode(&doc); err != nil {
		return nil, "", err
	}

	id, ok := doc[ee.idPropertyName].(string)
	if !ok {
		return nil, "", errors.New("no id found in document")
	}

	return doc, id, nil
}

func (ee elasticEngine) Check() error {
	return errors.New("check not implemented")
}

func (ee elasticEngine) Initialise() error {
	req, err := http.NewRequest("PUT", fmt.Sprintf("%s/%s/_settings", ee.baseURL, ee.indexName), strings.NewReader(`{ "index" : { "max_result_window" : 500000 } }`))
	if err != nil {
		return err
	}
	resp, err := ee.client.Do(req)
	if err != nil {
		return err
	}
	defer func() {
		io.Copy(ioutil.Discard, resp.Body)
		_ = resp.Body.Close()
	}()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("failed to apply index settings : %s", resp.Status)
	}

	return nil
}

func (ee elasticEngine) Close() {
}

type esSearchResult struct {
	Hits hitResult `json:"hits"`
}
type hitResult struct {
	Hits []esSearchID `json:"hits"`
}
type esSearchID struct {
	ID string `json:"_id"`
}
