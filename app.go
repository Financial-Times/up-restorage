package main

import (
	"encoding/json"
	"fmt"
	"github.com/gorilla/handlers"
	"github.com/gorilla/mux"
	"github.com/jawher/mow.cli"
	"io"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"strings"
	"sync"
)

func parseCollections(mappings string) map[string]Collection {
	idMapping := make(map[string]Collection)
	for _, mapping := range strings.Split(mappings, ",") {
		kv := strings.Split(mapping, ":")
		if len(kv) != 2 {
			log.Printf("can't parse id mapping %s, skipping\n", mapping)
		} else {
			idMapping[kv[0]] = Collection{name: kv[0], idPropertyName: kv[1]}
		}
	}

	log.Printf("collection identifier mappings are:\n")
	for k, v := range idMapping {
		log.Printf("%s : %s", k, v)
	}
	return idMapping
}

func main() {

	app := cli.App("restorage", "A RESTful storage API with pluggable backends")
	port := app.IntOpt("port", 8080, "Port to listen on")
	idMap := app.StringOpt("id-map", "test1:uuid,test2:id,...", "Mapping of collection name to identifier property name")

	app.Command("elastic", "use the elastic search backend", func(cmd *cli.Cmd) {
		url := cmd.StringArg("URL", "", "elastic search endpoint url")
		indexName := cmd.StringOpt("index-name", "store", "elastic search index name")
		cmd.Action = func() {
			serve(NewElasticEngine(*url, *indexName), parseCollections(*idMap), *port)
		}
	})

	app.Command("mongo", "use the mongodb backend", func(cmd *cli.Cmd) {
		hostports := cmd.StringArg("HOSTS", "", "hostname1:port1,hostname2:port2,...")
		dbname := cmd.StringOpt("dbname", "store", "database name")
		isBinaryId := cmd.BoolOpt("binary-identity", false, "Is the configured id in a binary format?")
		cmd.Action = func() {
			colls := parseCollections(*idMap)
			serve(NewMongoEngine(*dbname, colls, *hostports, *isBinaryId), colls, *port)
		}
	})

	app.Run(os.Args)

}

func serve(engine Engine, collections map[string]Collection, port int) {
	ah := apiHandlers{engine, collections}

	m := mux.NewRouter()
	http.Handle("/", handlers.CombinedLoggingHandler(os.Stdout, m))

	// count
	m.HandleFunc("/{collection}/__count", ah.countHandler).Methods("GET")

	// {"id":"e1cd2aa4-c5bb-46b2-b677-846640f22428"}{"id":"f8e46a87-5514-48fb-a6b2-f82d3cf11e92"} style response
	m.HandleFunc("/{collection}/__ids", ah.idsHandler).Methods("GET")

	// get by id and get all
	m.HandleFunc("/{collection}/{id}", ah.idReadHandler).Methods("GET")
	m.HandleFunc("/{collection}/", ah.dumpAll).Methods("GET")

	// put by id and put all
	m.HandleFunc("/{collection}/{id}", ah.idWriteHandler).Methods("PUT")
	m.HandleFunc("/{collection}/", ah.putAllHandler).Methods("PUT")

	// delete by id and delete all
	m.HandleFunc("/{collection}/{id}", ah.idDeleteHandler).Methods("DELETE")
	m.HandleFunc("/{collection}/", ah.dropHandler).Methods("DELETE")

	go func() {
		fmt.Printf("listening on %d\n", port)
		err := http.ListenAndServe(fmt.Sprintf(":%d", port), nil)
		if err != nil {
			log.Printf("web server failed: %v\n", err)
		}
	}()

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)

	// wait for ctrl-c
	<-c
	println("exiting")
	engine.Close()

	return
}

type apiHandlers struct {
	engine Engine
	colls  map[string]Collection
}

func (ah *apiHandlers) idReadHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	id := vars["id"]
	coll, err := ah.getCollection(vars["collection"])
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	found, art, err := ah.engine.Read(coll, id)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
	if !found {
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte(fmt.Sprintf("document with id %s was not found\n", id)))
		return
	}
	w.Header().Add("Content-Type", "application/json")
	enc := json.NewEncoder(w)
	enc.Encode(art)
}

func (ah *apiHandlers) putAllHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	coll, err := ah.getCollection(vars["collection"])
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	errCh := make(chan error, 2)
	docCh := make(chan Document)

	var wg sync.WaitGroup

	wg.Add(1)

	go func() {
		defer wg.Done()
		defer close(docCh)

		dec := json.NewDecoder(r.Body) //TODO: bufio?
		for {
			var doc Document
			err := dec.Decode(&doc)
			if err == io.EOF {
				return
			}
			if err != nil {
				errCh <- err
				log.Printf("failed to decode json. aborting: %v\n", err.Error())
				return
			}
			docCh <- doc
		}

	}()

	for x := 0; x < 8; x++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for doc := range docCh {
				err := ah.engine.Write(coll, getID(coll, doc), doc)
				if err != nil {
					errCh <- err
					return
				}
			}
		}()
	}

	wg.Wait()

	select {
	case err := <-errCh:
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	default:
		println("returning normally")
		return
	}

}

func getID(coll Collection, doc Document) string {
	if id, ok := doc[coll.idPropertyName].(string); ok {
		return id
	}
	panic("no id")
}

func (ah *apiHandlers) idWriteHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	id := vars["id"]

	coll, err := ah.getCollection(vars["collection"])
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	var doc Document
	dec := json.NewDecoder(r.Body)
	if err := dec.Decode(&doc); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if getID(coll, doc) != id {
		http.Error(w, "id does not match", http.StatusBadRequest)
		return
	}

	err = ah.engine.Write(coll, id, doc)
	if err != nil {
		http.Error(w, fmt.Sprintf("write failed:\n%v\n", err), http.StatusInternalServerError)
		return
	}
}

func (ah *apiHandlers) idDeleteHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	id := vars["id"]

	coll, err := ah.getCollection(vars["collection"])
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	err = ah.engine.Delete(coll, id)
	if err != nil {
		http.Error(w, fmt.Sprintf("delete failed:\n%v\n", err), http.StatusInternalServerError)
		return
	}
}

func (ah *apiHandlers) getCollection(name string) (Collection, error) {
	coll := ah.colls[name]
	if coll == (Collection{}) {
		return coll, fmt.Errorf("unknown collection %s", name)
	}
	return coll, nil
}

func (ah *apiHandlers) dropHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	coll, err := ah.getCollection(vars["collection"])
	if err != nil {

		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	ok, err := ah.engine.Drop(coll)

	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if !ok {
		w.WriteHeader(http.StatusNoContent)
		return
	}
}

func (ah *apiHandlers) dumpAll(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	coll, err := ah.getCollection(vars["collection"])

	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	enc := json.NewEncoder(w)
	stop := make(chan struct{})
	defer close(stop)
	all, err := ah.engine.All(coll, stop)
	if err != nil {
		switch {
		case err == ErrInvalidQuery:
			w.WriteHeader(http.StatusBadRequest)
		case err == ErrNotFound:
			w.WriteHeader(http.StatusNotFound)
		default:
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
		return
	}
	for doc := range all {
		enc.Encode(doc)
		fmt.Fprint(w, "\n")
	}
}

func (ah *apiHandlers) idsHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	coll, err := ah.getCollection(vars["collection"])
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	enc := json.NewEncoder(w)
	stop := make(chan struct{})
	defer close(stop)
	all, err := ah.engine.Ids(coll, stop)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	id := struct {
		ID string `json:"id"`
	}{}
	for docId := range all {
		id.ID = docId
		enc.Encode(id)
	}
}

func (ah *apiHandlers) countHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	coll, err := ah.getCollection(vars["collection"])
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	count, err := ah.engine.Count(coll)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	fmt.Fprintf(w, "%d\n", count)
}
