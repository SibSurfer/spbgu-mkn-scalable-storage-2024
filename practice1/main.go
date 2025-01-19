package main

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/paulmach/orb/geojson"
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
	return &result
}

func (r *Router) Run() {

}

func (r *Router) Stop() {

}

type Storage struct {
	name string
	dir  string
	mtx  sync.Mutex
}

func writeError(w http.ResponseWriter, err error) {
	w.WriteHeader(http.StatusInternalServerError)
	w.Write([]byte(err.Error()))
}

func getFeature(r *http.Request) (*geojson.Feature, string, error) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		slog.Error("failed to get body")
		return nil, "", err
	}
	f, err := geojson.UnmarshalFeature(body)
	if err != nil {
		slog.Error("unmarshall failed")
		return nil, "", err
	}
	id, ok := f.ID.(string)
	if !ok {
		slog.Error("id failed")
		return nil, "", errors.New("id not a string here")
	}
	return f, id, nil
}

func NewStorage(mux *http.ServeMux, name string, replicas []string, leader bool) *Storage {
	wd, err := os.Getwd()
	if err != nil {
		panic("getwd fail")
	}
	dir := wd + "/" + name
	result := Storage{name, dir, sync.Mutex{}}
	mux.HandleFunc("/"+name+"/select", func(w http.ResponseWriter, r *http.Request) {
		slog.Info("select")
		collection := geojson.NewFeatureCollection()
		result.mtx.Lock()
		entries, err := os.ReadDir(dir)
		if err != nil {
			writeError(w, err)
			return
		}
		for _, entry := range entries {
			data, err := os.ReadFile(dir + "/" + entry.Name())
			if err != nil {
				writeError(w, err)
				return
			}
			f := geojson.Feature{}
			err = f.UnmarshalBSON(data)
			if err != nil {
				writeError(w, err)
				return
			}
			collection.Append(&f)
		}
		result.mtx.Unlock()
		data, err := collection.MarshalJSON()
		if err != nil {
			writeError(w, err)
			return
		}
		w.WriteHeader(http.StatusOK)
		w.Write(data)
	})
	mux.HandleFunc("/"+name+"/insert", func(w http.ResponseWriter, r *http.Request) {
		slog.Info("insert")
		f, id, err := getFeature(r)
		if err != nil {
			writeError(w, err)
			return
		}
		result.mtx.Lock()
		json, err := f.MarshalBSON()
		if err != nil {
			writeError(w, err)
			return
		}
		err = os.WriteFile(dir+"/"+id+".json", json, 0666)
		if err != nil {
			writeError(w, err)
			return
		}
		result.mtx.Unlock()
		w.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("/"+name+"/replace", func(w http.ResponseWriter, r *http.Request) {
		slog.Info("replace")
		f, id, err := getFeature(r)
		if err != nil {
			writeError(w, err)
			return
		}
		result.mtx.Lock()
		json, err := f.MarshalBSON()
		if err != nil {
			writeError(w, err)
			return
		}
		err = os.WriteFile(dir+"/"+id+".json", json, 0666)
		if err != nil {
			writeError(w, err)
			return
		}
		result.mtx.Unlock()
		w.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("/"+name+"/delete", func(w http.ResponseWriter, r *http.Request) {
		slog.Info("delete")
		_, id, err := getFeature(r)
		if err != nil {
			writeError(w, err)
			return
		}
		result.mtx.Lock()
		err = os.Remove(dir + "/" + id + ".json")
		if err != nil {
			writeError(w, err)
			return
		}
		result.mtx.Unlock()
		w.WriteHeader(http.StatusOK)
	})
	return &result
}

func (s *Storage) Run() {
	err := os.MkdirAll(s.dir, 0777)
	if err != nil {
		panic("mkdirall failed")
	}
}
func (s *Storage) Stop() {
	err := os.RemoveAll(s.dir)
	if err != nil {
		panic("failed to remove")
	}
}

func main() {
	mux := http.ServeMux{}

	storage := NewStorage(&mux, "test", []string{}, true)
	storage.Run()

	router := NewRouter(&mux, [][]string{{"test"}})
	router.Run()

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
	router.Stop()
	storage.Stop()
}
