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
	"os"
	"os/signal"
	"runtime/pprof"
	"sync"
)

func main() {

	f, err := os.Create("/tmp/cpuprof")
	if err != nil {
		panic(err)
	}
	pprof.StartCPUProfile(f)
	defer pprof.StopCPUProfile()

	app := cli.App("restorage", "A RESTful storage API with pluggable backends")
	port := app.IntOpt("port", 8080, "Port to listen on")

	app.Command("elastic", "use the elastic search backend", func(cmd *cli.Cmd) {
		url := cmd.StringArg("URL", "", "elastic search endpoint url")
		indexName := cmd.StringOpt("index-name", "store", "elastic search index name")
		cmd.Action = func() {
			println(*url)
			serve(NewElasticEngine(*url, *indexName), *port)
		}

	})

	app.Command("mongo", "use the mongodb backend", func(cmd *cli.Cmd) {
		hostports := cmd.StringArg("HOSTS", "", "hostname1:port1,hostname2:port2...")
		dbname := cmd.StringOpt("dbname", "store", "database name")
		cmd.Action = func() {
			serve(NewMongoEngine(*dbname, *hostports), *port)
		}

	})

	app.Run(os.Args)

}

func serve(engine Engine, port int) {
	ah := apiHandlers{engine}

	m := mux.NewRouter()
	http.Handle("/", handlers.CombinedLoggingHandler(os.Stdout, m))

	m.HandleFunc("/{collection}/_count", ah.countHandler).Methods("GET")
	m.HandleFunc("/{collection}/{id}", ah.idReadHandler).Methods("GET")
	m.HandleFunc("/{collection}/{id}", ah.idWriteHandler).Methods("PUT")
	m.HandleFunc("/{collection}/", ah.dropHandler).Methods("DELETE")
	m.HandleFunc("/{collection}/", ah.putAllHandler).Methods("PUT")
	m.HandleFunc("/{collection}/", ah.dumpAll).Methods("GET")

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

	f, err := os.Create("/tmp/memprof")
	if err != nil {
		panic(err)
	}

	pprof.WriteHeapProfile(f)
	f.Close()

	return
}

type apiHandlers struct {
	engine Engine
}

func (ah *apiHandlers) idReadHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	id := vars["id"]
	collection := vars["collection"]

	found, art, err := ah.engine.Load(collection, id)
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
	collection := vars["collection"]

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
				err := ah.engine.Write(collection, getID(doc), doc)
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

func getID(doc Document) string {
	// TODO: obviously this should be parameterised
	if id, ok := doc["uuid"].(string); ok {
		return id
	}
	if id, ok := doc["id"].(string); ok {
		return id
	}
	panic("no id")
}

func (ah *apiHandlers) idWriteHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	id := vars["id"]
	collection := vars["collection"]

	var doc Document
	dec := json.NewDecoder(r.Body)
	err := dec.Decode(&doc)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if getID(doc) != id {
		http.Error(w, "id does not match", http.StatusBadRequest)
		return
	}

	err = ah.engine.Write(collection, id, doc)
	if err != nil {
		http.Error(w, fmt.Sprintf("write failed:\n%v\n", err), http.StatusInternalServerError)
		return
	}
}

func (ah *apiHandlers) dropHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	collection := vars["collection"]
	ah.engine.Drop(collection)
}

func (ah *apiHandlers) dumpAll(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	collection := vars["collection"]

	enc := json.NewEncoder(w)
	stop := make(chan struct{})
	defer close(stop)
	all, err := ah.engine.All(collection, stop)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	for doc := range all {
		enc.Encode(doc)
		fmt.Fprint(w, "\n")
	}
}

func (ah *apiHandlers) countHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	collection := vars["collection"]
	fmt.Fprintf(w, "%d\n", ah.engine.Count(collection))
}
