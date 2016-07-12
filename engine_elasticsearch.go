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
	client         *http.Client
	baseURL        string
	indexName      string
	collectionName string
	idPropertyName string
}

func NewElasticEngine(elasticURL string, indexName string, collectionName string, idPropertyName string, client *http.Client) Engine {
	e := &elasticEngine{
		client:    client,
		baseURL:   elasticURL,
		indexName: indexName,
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

func (ee *elasticEngine) Write(cont Document) error {
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
	io.Copy(ioutil.Discard, resp.Body)
	resp.Body.Close()

	select {
	case e := <-writeErr:
		return e
	case <-doneWrite:
		return nil
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

func (ee *elasticEngine) Read(id string) (bool, Document, error) {
	res, err := ee.client.Get(fmt.Sprintf("%s/%s/%s/%s", ee.baseURL, ee.indexName, ee.collectionName, id))
	if err != nil {
		return false, Document{}, err
	}
	defer res.Body.Close()
	switch {
	case res.StatusCode == 200:
		dec := json.NewDecoder(res.Body)
		var result esGetResult
		err := dec.Decode(&result)
		if err != nil {
			return false, Document{}, err
		}
		return true, result.Source, nil
	case res.StatusCode == 404:
		return false, Document{}, nil
	default:
		return false, Document{}, fmt.Errorf("read fail: %s", res.Status)
	}
}

type esGetResult struct {
	Source Document `json:"_source"`
}

func (ee elasticEngine) All(closechan chan struct{}) (chan Document, error) {
	count, err := ee.Count()
	if err != nil {
		return nil, err
	}
	q := fmt.Sprintf("{\"query\":{\"match_all\": {}}, \"fields\":[], \"size\": %d,  \"from\": 0}", count+1000)
	return ee.query(q, closechan)
}

func (ee elasticEngine) Ids(stopchan chan struct{}) (chan string, error) {
	panic("not implemented")
}

func (ee elasticEngine) IDPropertyName() string {
	return ee.idPropertyName
}

func (ee elasticEngine) query(q string, closechan chan struct{}) (chan Document, error) {
	cont := make(chan Document)
	res, err := ee.client.Post(fmt.Sprintf("%s/%s/%s/_search", ee.baseURL, ee.indexName, ee.collectionName), "application/json", strings.NewReader(q))
	if err != nil {
		return nil, err
	}
	switch {
	case res.StatusCode == 200:
	case res.StatusCode == 400:
		res.Body.Close()
		return nil, ErrInvalidQuery
	case res.StatusCode == 404:
		return nil, ErrNotFound
	default:
		return nil, fmt.Errorf("query failed: %s", res.Status)
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
			found, c, err := ee.Read(id)
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
