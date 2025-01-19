package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/paulmach/orb/geojson"
	"github.com/tidwall/rtree"
)

type Router struct {
}

func NewRouter(mux *http.ServeMux, nodes [][]string) *Router {
	result := Router{}

	mux.Handle("/", http.FileServer(http.Dir("../front/dist")))
	mux.Handle("/select", http.RedirectHandler("/"+nodes[0][0]+"/select", http.StatusTemporaryRedirect))
	mux.Handle("/insert", http.RedirectHandler("/"+nodes[0][0]+"/insert", http.StatusTemporaryRedirect))
	mux.Handle("/replace", http.RedirectHandler("/"+nodes[0][0]+"/replace", http.StatusTemporaryRedirect))
	mux.Handle("/delete", http.RedirectHandler("/"+nodes[0][0]+"/delete", http.StatusTemporaryRedirect))
	mux.Handle("/checkpoint", http.RedirectHandler("/"+nodes[0][0]+"/checkpoint", http.StatusTemporaryRedirect))
	return &result
}

func (r *Router) Run()  {}
func (r *Router) Stop() {}

type Message struct {
	action string
	name   string
	data   []byte
	cb     chan any
}

type Transaction struct {
	Action  string
	Name    string
	Feature geojson.Feature
}

type Storage struct {
	name   string
	ctx    context.Context
	cancel context.CancelFunc
	queue  chan Message
}

func writeError(w http.ResponseWriter, err error) {
	w.WriteHeader(http.StatusInternalServerError)
	w.Write([]byte(err.Error()))
}

func handleRequest(action string, name string, queue chan Message, r *http.Request) (any, error) {
	slog.Info(action)
	body, err := io.ReadAll(r.Body)
	if err != nil {
		return nil, err
	}
	cb := make(chan any)
	queue <- Message{action, name, body, cb}
	res := <-cb
	close(cb)
	if err, ok := res.(error); ok {
		return nil, err
	}
	return res, nil
}

func handlePost(action string, name string, queue chan Message, w http.ResponseWriter, r *http.Request) {
	_, err := handleRequest(action, name, queue, r)
	if err != nil {
		writeError(w, err)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func NewStorage(mux *http.ServeMux, name string, replicas []string, leader bool) *Storage {
	ctx, cancel := context.WithCancel(context.Background())
	queue := make(chan Message)
	result := Storage{name, ctx, cancel, queue}
	mux.HandleFunc("/"+name+"/select", func(w http.ResponseWriter, r *http.Request) {
		res, err := handleRequest("select", name, queue, r)
		if err != nil {
			writeError(w, err)
			return
		}
		collection, ok := res.(*geojson.FeatureCollection)
		if !ok {
			writeError(w, err)
			return
		}
		json, err := collection.MarshalJSON()
		if err != nil {
			writeError(w, err)
			return
		}
		w.WriteHeader(http.StatusOK)
		w.Write(json)
	})
	mux.HandleFunc("/"+name+"/insert", func(w http.ResponseWriter, r *http.Request) {
		handlePost("insert", name, queue, w, r)
	})
	mux.HandleFunc("/"+name+"/replace", func(w http.ResponseWriter, r *http.Request) {
		handlePost("replace", name, queue, w, r)
	})
	mux.HandleFunc("/"+name+"/delete", func(w http.ResponseWriter, r *http.Request) {
		handlePost("delete", name, queue, w, r)
	})
	mux.HandleFunc("/"+name+"/checkpoint", func(w http.ResponseWriter, r *http.Request) {
		handlePost("checkpoint", name, queue, w, r)
	})
	return &result
}

func getFeature(data []byte) (*geojson.Feature, string, error) {
	f, err := geojson.UnmarshalFeature(data)
	if err != nil {
		slog.Error("unmarshall fail")
		return nil, "", err
	}
	id, ok := f.ID.(string)
	if !ok {
		slog.Error("id fail")
		return nil, "", errors.New("id not a string here")
	}
	return f, id, nil
}

func (s *Storage) Run() {
	go func() {
		makeCheckpoint := func(features map[string]*geojson.Feature) error {
			collection := geojson.NewFeatureCollection()
			for _, f := range features {
				collection.Append(f)
			}
			json, err := collection.MarshalJSON()
			if err != nil {
				return err
			}
			err = os.WriteFile(s.name+".ckp", json, 0666)
			if err != nil {
				return err
			}
			err = os.WriteFile(s.name+".wal", []byte{}, 0666)
			return err
		}
		makeTransaction := func(action string, f *geojson.Feature) error {
			transaction := Transaction{action, s.name, *f}
			json, err := json.Marshal(transaction)
			if err != nil {
				return err
			}
			wal, err := os.OpenFile(s.name+".wal", os.O_APPEND|os.O_WRONLY, 0644)
			if err != nil {
				return err
			}
			defer wal.Close()
			_, err = wal.Write(json)
			return err
		}
		init := func() (map[string]*geojson.Feature, error) {
			features := make(map[string]*geojson.Feature)
			defer makeCheckpoint(features)
			wal, err := os.ReadFile(s.name + ".wal")
			if err != nil {
				return features, nil
			}
			ckp, err := os.ReadFile(s.name + ".ckp")
			if err == nil {
				collection, err := geojson.UnmarshalFeatureCollection(ckp)
				if err != nil {
					return nil, err
				}
				for _, f := range collection.Features {
					features[f.ID.(string)] = f
				}
			}
			decoder := json.NewDecoder(bytes.NewReader(wal))
			for decoder.More() {
				transaction := Transaction{}
				err := decoder.Decode(&transaction)
				if err != nil {
					return nil, err
				}
				switch transaction.Action {
				case "insert":
					features[transaction.Feature.ID.(string)] = &transaction.Feature
				case "replace":
					features[transaction.Feature.ID.(string)] = &transaction.Feature
				case "delete":
					delete(features, transaction.Feature.ID.(string))
				}
			}
			return features, nil
		}
		features, err := init()
		index := rtree.RTree{}
		for _, f := range features {
			index.Insert(f.Geometry.Bound().Min, f.Geometry.Bound().Max, f)
		}
		if err != nil {
			panic(err)
		}
		for {
			select {
			case <-s.ctx.Done():
				return
			case msg := <-s.queue:
				switch msg.action {
				case "select":
					collection := geojson.NewFeatureCollection()
					for _, f := range features {
						collection.Append(f)
					}
					msg.cb <- collection
				case "insert":
					f, id, err := getFeature(msg.data)
					if err != nil {
						msg.cb <- err
						continue
					}
					err = makeTransaction("insert", f)
					if err != nil {
						msg.cb <- err
						continue
					}
					index.Insert(f.Geometry.Bound().Min, f.Geometry.Bound().Max, f)
					features[id] = f
					msg.cb <- nil
				case "replace":
					f, id, err := getFeature(msg.data)
					if err != nil {
						msg.cb <- err
						continue
					}
					err = makeTransaction("replace", f)
					if err != nil {
						msg.cb <- err
						continue
					}
					old, ok := features[id]
					if !ok {
						msg.cb <- errors.New(id + " not exists")
						continue
					}
					index.Delete(old.Geometry.Bound().Min, old.Geometry.Bound().Max, old)
					index.Insert(f.Geometry.Bound().Min, f.Geometry.Bound().Max, f)
					features[id] = f
					msg.cb <- nil
				case "delete":
					f, id, err := getFeature(msg.data)
					if err != nil {
						msg.cb <- err
						continue
					}
					err = makeTransaction("delete", f)
					if err != nil {
						msg.cb <- err
						continue
					}
					index.Delete(f.Geometry.Bound().Min, f.Geometry.Bound().Max, f)
					delete(features, id)
					msg.cb <- nil
				case "checkpoint":
					msg.cb <- makeCheckpoint(features)
				default:
					panic("operation unrecognised")
				}
			}
		}
	}()
}

func (s *Storage) Stop() {
	s.cancel()
}

func main() {
	mux := http.ServeMux{}

	storage := NewStorage(&mux, "test", []string{}, true)
	storage.Run()
	defer storage.Stop()

	router := NewRouter(&mux, [][]string{{"test"}})
	router.Run()
	defer router.Stop()

	server := http.Server{}
	server.Addr = "127.0.0.1:8080"
	server.Handler = &mux

	go func() {
		sigs := make(chan os.Signal, 1)
		signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
		sig := <-sigs
		slog.Info("Got signal " + sig.String())
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		server.Shutdown(ctx)
	}()
	defer slog.Info("Stopped")
	slog.Info("Listen http://" + server.Addr)

	err := server.ListenAndServe()
	if !errors.Is(err, http.ErrServerClosed) {
		slog.Info("err", "err", err)
	}
}
