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
	client  *http.Client
	baseURL string
}

func NewElasticEngine(elasticURL string) Engine {
	transport := &http.Transport{MaxIdleConnsPerHost: 30}
	e := &elasticEngine{
		client:  &http.Client{Transport: transport},
		baseURL: elasticURL,
	}
	for strings.HasSuffix(e.baseURL, "/") {
		e.baseURL = e.baseURL[0 : len(e.baseURL)-1]
	}

	return e
}

func (ee *elasticEngine) Drop(collection string) {
	req, err := http.NewRequest("DELETE", fmt.Sprintf("%s/store/%s/", ee.baseURL, collection), nil)
	if err != nil {
		panic(err)
	}
	resp, err := ee.client.Do(req)
	if err != nil {
		panic(err)
	}
	if resp.StatusCode != 200 && resp.StatusCode != 404 {
		panic("drop fail")
	}
}

func (ee *elasticEngine) Write(collection, id string, cont Document) error {
	if id == "" {
		return errors.New("missing id")
	}

	r, w := io.Pipe()
	go func() {
		je := json.NewEncoder(w)
		err := je.Encode(cont)
		if err != nil {
			panic(err)
		}
		w.Close()
	}()

	req, err := http.NewRequest("PUT", fmt.Sprintf("%s/store/%s/%s", ee.baseURL, collection, id), r)
	if err != nil {
		panic(err)
	}
	resp, err := ee.client.Do(req)
	if err != nil {
		panic(err)
	}
	io.Copy(ioutil.Discard, resp.Body)
	resp.Body.Close()

	return nil
}

func (ee *elasticEngine) Count(collection string) int {
	res, err := ee.client.Get(fmt.Sprintf("%s/store/%s/_count", ee.baseURL, collection))
	if err != nil {
		panic(err)
	}
	defer res.Body.Close()
	if res.StatusCode == 200 {
		dec := json.NewDecoder(res.Body)
		var result esCountResult
		err := dec.Decode(&result)
		if err != nil {
			panic(err)
		}
		return result.Count
	}
	panic(res.StatusCode)
}

type esCountResult struct {
	Count int `json:"count"`
}

func (ee *elasticEngine) Load(collection, id string) (bool, Document, error) {
	res, err := ee.client.Get(fmt.Sprintf("%s/store/%s/%s", ee.baseURL, collection, id))
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

func (ee elasticEngine) All(collection string, closechan chan struct{}) (chan Document, error) {
	q := fmt.Sprintf("{\"query\":{\"match_all\": {}}, \"fields\":[], \"size\": %d,  \"from\": 0}", ee.Count(collection)+1000)
	return ee.query(collection, q, closechan)
}

func (ee elasticEngine) query(collection, q string, closechan chan struct{}) (chan Document, error) {
	cont := make(chan Document)

	res, err := ee.client.Post(fmt.Sprintf("%s/store/%s/_search", ee.baseURL, collection), "application/json", strings.NewReader(q))
	if err != nil {
		panic(err)
	}

	switch {
	case res.StatusCode == 200:
	case res.StatusCode == 400:
		res.Body.Close()
		return nil, ErrInvalidQuery
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
			id := h.Id
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
	Hits []esSearchId `json:"hits"`
}
type esSearchId struct {
	Id string `json:"_id"`
}
