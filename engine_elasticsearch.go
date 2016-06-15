package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"strings"
)

type elasticEngine struct {
	client    *http.Client
	baseURL   string
	indexName string
}

func NewElasticEngine(elasticURL string, indexName string) Engine {
	transport := &http.Transport{MaxIdleConnsPerHost: 30}
	e := &elasticEngine{
		client:    &http.Client{Transport: transport},
		baseURL:   elasticURL,
		indexName: indexName,
	}
	for strings.HasSuffix(e.baseURL, "/") {
		e.baseURL = e.baseURL[0 : len(e.baseURL)-1]
	}

	return e
}

func (ee *elasticEngine) Drop(collection Collection) (bool, error) {
	req, err := http.NewRequest("DELETE", fmt.Sprintf("%s/%s?_type=%s", ee.baseURL, ee.indexName, collection.name), nil)
	if err != nil {
		panic(err)
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
		panic("drop fail")
	}
}

func (ee *elasticEngine) Write(collection Collection, id string, cont Document) error {
	if id == "" {
		return errors.New("missing id")
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

	u := fmt.Sprintf("%s/%s/%s/%s", ee.baseURL, ee.indexName, collection.name, id)
	req, err := http.NewRequest("PUT", u, r)
	if err != nil {
		return err
	}
	resp, err := ee.client.Do(req)
	if err != nil {
		return err
	}
	io.Copy(ioutil.Discard, resp.Body)
	resp.Body.Close()

	select {
	case e := <-writeErr:
		return e
	case <-doneWrite:
		return nil
	}
}

func (ee *elasticEngine) Delete(collection Collection, id string) error {
	if id == "" {
		return errors.New("missing id")
	}

	req, err := http.NewRequest("DELETE", fmt.Sprintf("%s/%s/%s/%s", ee.baseURL, ee.indexName, collection.name, id), nil)
	if err != nil {
		panic(err)
	}
	resp, err := ee.client.Do(req)
	if err != nil {
		fmt.Printf("e: %v\n", err)
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 400 && resp.StatusCode != 404 {
		return fmt.Errorf("delete request failed with status %d", resp.Status)
	}

	return nil
}

func (ee *elasticEngine) Count(collection Collection) (int, error) {
	res, err := ee.client.Get(fmt.Sprintf("%s/%s/%s/_count", ee.baseURL, ee.indexName, collection.name))
	if err != nil {
		panic(err)
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
	panic(res.StatusCode)
}

type esCountResult struct {
	Count int `json:"count"`
}

func (ee *elasticEngine) Load(collection Collection, id string) (bool, Document, error) {
	res, err := ee.client.Get(fmt.Sprintf("%s/%s/%s/%s", ee.baseURL, ee.indexName, collection.name, id))
	if err != nil {
		panic(err)
	}
	defer res.Body.Close()
	switch {
	case res.StatusCode == 200:
		dec := json.NewDecoder(res.Body)
		var result esGetResult
		err := dec.Decode(&result)
		if err != nil {
			panic(err)
		}
		return true, result.Source, nil
	case res.StatusCode == 404:
		return false, Document{}, nil
	default:
		panic(res.StatusCode)
	}
}

type esGetResult struct {
	Source Document `json:"_source"`
}

func (ee elasticEngine) All(collection Collection, closechan chan struct{}) (chan Document, error) {
	count, err := ee.Count(collection)
	if err != nil {
		return nil, err
	}
	q := fmt.Sprintf("{\"query\":{\"match_all\": {}}, \"fields\":[], \"size\": %d,  \"from\": 0}", count+1000)
	return ee.query(collection, q, closechan)
}

func (ee elasticEngine) Ids(collection Collection, stopchan chan struct{}) (chan string, error) {
	panic("not implemented")
}

func (ee elasticEngine) query(collection Collection, q string, closechan chan struct{}) (chan Document, error) {
	cont := make(chan Document)
	res, err := ee.client.Post(fmt.Sprintf("%s/%s/%s/_search", ee.baseURL, ee.indexName, collection.name), "application/json", strings.NewReader(q))
	if err != nil {
		panic(err)
	}
	switch {
	case res.StatusCode == 200:
	case res.StatusCode == 400:
		res.Body.Close()
		return nil, ErrInvalidQuery
	case res.StatusCode == 404:
		return nil, ErrNotFound
	default:
		panic(res.StatusCode)
	}

	go func() {
		defer close(cont)

		defer res.Body.Close()
		dec := json.NewDecoder(res.Body)
		var result esSearchResult
		err := dec.Decode(&result)
		if err != nil {
			panic(err)
		}
		for _, h := range result.Hits.Hits {
			id := h.ID
			found, c, err := ee.Load(collection, id)
			if err != nil {
				panic(err)
			}
			if !found {
				panic("fail")
			}
			select {
			case cont <- c:
			case <-closechan:
				break
			}
		}
	}()

	return cont, nil
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
