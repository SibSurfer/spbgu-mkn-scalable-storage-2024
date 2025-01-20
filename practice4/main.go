package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/gorilla/websocket"
	"github.com/paulmach/orb/geojson"
	"github.com/tidwall/rtree"
)

type Shard struct {
	leader   string
	replicas []string
}

type Router struct {
	index rtree.RTree
	url   string
}

func (rt *Router) getPostHandler(action string) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		json, err := io.ReadAll(r.Body)
		if err != nil {
			respondWithError(w, err)
			return
		}
		f, _, err := extractFeature(json)
		if err != nil {
			respondWithError(w, err)
			return
		}
		rt.index.Search(f.Geometry.Bound().Min, f.Geometry.Bound().Min, func(min, max [2]float64, data interface{}) bool {
			shard := data.(Shard)
			http.Redirect(w, r, "/"+shard.leader+"/"+action, http.StatusTemporaryRedirect)
			return true
		})
	}
}

func getRectValues(r *http.Request) ([2]float64, [2]float64, error) {
	get := r.URL.Query().Get("rect")
	rect := strings.Split(get, ",")
	if len(rect) < 4 {
		return [2]float64{}, [2]float64{}, errors.New("4 nums expected")
	}
	lowerLeftX, err := strconv.ParseFloat(rect[0], 64)
	if err != nil {
		return [2]float64{}, [2]float64{}, err
	}
	lowerLeftY, err := strconv.ParseFloat(rect[1], 64)
	if err != nil {
		return [2]float64{}, [2]float64{}, err
	}
	upperRightX, err := strconv.ParseFloat(rect[2], 64)
	if err != nil {
		return [2]float64{}, [2]float64{}, err
	}
	upperRightY, err := strconv.ParseFloat(rect[3], 64)
	if err != nil {
		return [2]float64{}, [2]float64{}, err
	}
	lowerLeft := [2]float64{lowerLeftX, lowerLeftY}
	upperRight := [2]float64{upperRightX, upperRightY}
	return lowerLeft, upperRight, nil
}

func NewRouter(mux *http.ServeMux, shards []Shard) *Router {
	result := Router{}
	n := 0
	for i := -180; i < 180; i += 10 {
		for j := -180; j < 180; j += 10 {
			var min [2]float64 = [2]float64{float64(j), float64(i)}
			var max [2]float64 = [2]float64{float64(j + 10), float64(i + 10)}
			result.index.Insert(min, max, shards[n])
			n++
			n %= len(shards)
		}
	}
	mux.Handle("/", http.FileServer(http.Dir("../front/dist")))
	mux.HandleFunc("/select", func(w http.ResponseWriter, r *http.Request) {
		min, max, err := getRectValues(r)
		if err != nil {
			respondWithError(w, err)
			return
		}
		collection := geojson.NewFeatureCollection()
		result.index.Search(min, max, func(min, max [2]float64, data interface{}) bool {
			shard := data.(Shard)
			n := rand.Intn(len(shard.replicas) + 1)
			var res *http.Response
			if n == 0 {
				res, _ = http.Get(result.url + "/" + shard.leader + "/select?" + r.URL.RawQuery)
			} else {
				n--
				res, _ = http.Get(result.url + "/" + shard.replicas[n] + "/select?" + r.URL.RawQuery)
			}
			json, _ := io.ReadAll(res.Body)
			tmp := geojson.NewFeatureCollection()
			tmp.UnmarshalJSON(json)
			for _, f := range tmp.Features {
				collection.Append(f)
			}
			return true
		})
		answer, err := collection.MarshalJSON()
		if err != nil {
			respondWithError(w, err)
			return
		}
		w.WriteHeader(http.StatusOK)
		w.Write(answer)
	})
	mux.HandleFunc("/insert", result.getPostHandler("insert"))
	mux.HandleFunc("/replace", result.getPostHandler("replace"))
	mux.HandleFunc("/delete", result.getPostHandler("delete"))
	mux.HandleFunc("/checkpoint", func(w http.ResponseWriter, r *http.Request) {
		for _, shard := range shards {
			http.Post(result.url+"/"+shard.leader+"/checkpoint", "application/json", bytes.NewReader([]byte{}))
			for _, replica := range shard.replicas {
				http.Post(result.url+"/"+replica+"/checkpoint", "application/json", bytes.NewReader([]byte{}))
			}
		}
	})
	return &result
}

func (r *Router) Run()  {}
func (r *Router) Stop() {}

type Message struct {
	action string
	data   []byte
	cb     chan any
}

type Transaction struct {
	Action  string
	Feature *geojson.Feature
}

type Storage struct {
	name     string
	replicas []*websocket.Conn
	leader   bool
	ctx      context.Context
	cancel   context.CancelFunc
	queue    chan Message
	features map[string]*geojson.Feature
	index    rtree.RTree
	mtx      sync.Mutex
}

func respondWithError(w http.ResponseWriter, err error) {
	w.WriteHeader(http.StatusInternalServerError)
	w.Write([]byte(err.Error()))
}

func handleRequest(action string, queue chan Message, r *http.Request) (any, error) {
	slog.Info("Handle " + action)
	body, err := io.ReadAll(r.Body)
	if err != nil {
		return nil, err
	}
	cb := make(chan any)
	queue <- Message{action, body, cb}
	res := <-cb
	close(cb)
	if err, ok := res.(error); ok {
		return nil, err
	}
	return res, nil
}

func handlePost(action string, queue chan Message, w http.ResponseWriter, r *http.Request) {
	_, err := handleRequest(action, queue, r)
	if err != nil {
		respondWithError(w, err)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func NewStorage(mux *http.ServeMux, name string, leader bool) *Storage {
	ctx, cancel := context.WithCancel(context.Background())
	queue := make(chan Message)
	result := Storage{name, make([]*websocket.Conn, 0), leader, ctx, cancel, queue, make(map[string]*geojson.Feature), rtree.RTree{}, sync.Mutex{}}
	mux.HandleFunc("/"+name+"/select", func(w http.ResponseWriter, r *http.Request) {
		res, err := handleRequest("select", queue, r)
		if err != nil {
			respondWithError(w, err)
			return
		}
		collection, ok := res.(*geojson.FeatureCollection)
		if !ok {
			respondWithError(w, err)
			return
		}
		json, err := collection.MarshalJSON()
		if err != nil {
			respondWithError(w, err)
			return
		}
		w.WriteHeader(http.StatusOK)
		w.Write(json)
	})
	if leader {
		mux.HandleFunc("/"+name+"/insert", func(w http.ResponseWriter, r *http.Request) {
			handlePost("insert", queue, w, r)
		})
		mux.HandleFunc("/"+name+"/replace", func(w http.ResponseWriter, r *http.Request) {
			handlePost("replace", queue, w, r)
		})
		mux.HandleFunc("/"+name+"/delete", func(w http.ResponseWriter, r *http.Request) {
			handlePost("delete", queue, w, r)
		})
	} else {
		mux.HandleFunc("/"+name+"/replication", func(w http.ResponseWriter, r *http.Request) {
			var upgrader = websocket.Upgrader{
				ReadBufferSize:  1024,
				WriteBufferSize: 1024,
			}
			ws, err := upgrader.Upgrade(w, r, nil)
			if err != nil {
				panic(err)
			}
			defer ws.Close()
			for {
				_, message, err := ws.ReadMessage()
				if err != nil {
					if err, ok := err.(*websocket.CloseError); ok && (err.Code == websocket.CloseNormalClosure) {
						return
					}
					panic(err)
				}
				transaction := Transaction{}
				err = json.Unmarshal(message, &transaction)
				if err != nil {
					panic(err)
				}
				body, err := transaction.Feature.MarshalJSON()
				if err != nil {
					panic(err)
				}
				cb := make(chan any)
				queue <- Message{transaction.Action, body, cb}
				<-cb
			}
		})
		mux.HandleFunc("/"+name+"/checkpoint", func(w http.ResponseWriter, r *http.Request) {
			handlePost("checkpoint", queue, w, r)
		})
	}
	return &result
}

func extractFeature(data []byte) (*geojson.Feature, string, error) {
	f, err := geojson.UnmarshalFeature(data)
	if err != nil {
		slog.Error("unmarshal fail")
		return nil, "", err
	}
	id, ok := f.ID.(string)
	if !ok {
		slog.Error("id fail")
		return nil, "", errors.New("id not string here")
	}
	return f, id, nil
}

func (s *Storage) makeCheckpoint() error {
	collection := geojson.NewFeatureCollection()
	for _, f := range s.features {
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

func (s *Storage) makeTransaction(action string, f *geojson.Feature) error {
	transaction := Transaction{action, f}
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

func (s *Storage) init() {
	defer s.makeCheckpoint()
	wal, err := os.ReadFile(s.name + ".wal")
	if err != nil {
		return
	}
	ckp, err := os.ReadFile(s.name + ".ckp")
	if err == nil {
		collection, err := geojson.UnmarshalFeatureCollection(ckp)
		if err != nil {
			panic(err)
		}
		for _, f := range collection.Features {
			s.features[f.ID.(string)] = f
		}
	}
	decoder := json.NewDecoder(bytes.NewReader(wal))
	for decoder.More() {
		transaction := Transaction{}
		err := decoder.Decode(&transaction)
		if err != nil {
			panic(err)
		}
		switch transaction.Action {
		case "insert":
			s.features[transaction.Feature.ID.(string)] = transaction.Feature
		case "replace":
			s.features[transaction.Feature.ID.(string)] = transaction.Feature
		case "delete":
			delete(s.features, transaction.Feature.ID.(string))
		}
	}
	for _, f := range s.features {
		s.index.Insert(f.BBox.Bound().Min, f.BBox.Bound().Max, f)
	}
}

func (s *Storage) replicateAction(action string, f *geojson.Feature) error {
	transaction := Transaction{action, f}
	json, err := json.Marshal(transaction)
	if err != nil {
		return err
	}
	for _, conn := range s.replicas {
		err = conn.WriteMessage(websocket.TextMessage, json)
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *Storage) initializeReplicaState(conn *websocket.Conn) {
	s.mtx.Lock()
	featuresCopy := make(map[string]*geojson.Feature)
	for k, v := range s.features {
		featuresCopy[k] = v
	}
	s.mtx.Unlock()

	for _, f := range featuresCopy {
		transaction := Transaction{"insert", f}
		json, err := json.Marshal(transaction)
		if err != nil {
			panic(err)
		}
		err = conn.WriteMessage(websocket.TextMessage, json)
		if err != nil {
			panic(err)
		}
	}
}

func (s *Storage) appendReplica(url string, name string) {
	if !s.leader {
		panic("not leader cant have replica")
	}
	conn, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(url, "http")+"/"+name+"/replication", nil)
	if err != nil {
		panic(err)
	}
	s.mtx.Lock()
	s.replicas = append(s.replicas, conn)
	s.mtx.Unlock()
	s.initializeReplicaState(conn)
}

func (s *Storage) handleMessage(msg Message) {
	s.mtx.Lock()
	defer s.mtx.Unlock()
	switch msg.action {
	case "select":
		collection := geojson.NewFeatureCollection()
		for _, f := range s.features {
			collection.Append(f)
		}
		msg.cb <- collection
	case "insert":
		f, id, err := extractFeature(msg.data)
		if err != nil {
			msg.cb <- err
		}
		err = s.makeTransaction("insert", f)
		if err != nil {
			msg.cb <- err
		}
		s.index.Insert(f.Geometry.Bound().Min, f.Geometry.Bound().Max, f)
		s.features[id] = f
		msg.cb <- nil
		if s.leader {
			s.replicateAction(msg.action, f)
		}
	case "replace":
		f, id, err := extractFeature(msg.data)
		if err != nil {
			msg.cb <- err
		}
		err = s.makeTransaction("replace", f)
		if err != nil {
			msg.cb <- err
		}
		old, ok := s.features[id]
		if !ok {
			msg.cb <- errors.New(id + " not exists")
		}
		s.index.Delete(old.Geometry.Bound().Min, old.Geometry.Bound().Max, old)
		s.index.Insert(f.Geometry.Bound().Min, f.Geometry.Bound().Max, f)
		s.features[id] = f
		msg.cb <- nil
		if s.leader {
			s.replicateAction(msg.action, f)
		}
	case "delete":
		f, id, err := extractFeature(msg.data)
		if err != nil {
			msg.cb <- err
		}
		err = s.makeTransaction("delete", f)
		if err != nil {
			msg.cb <- err
		}
		s.index.Delete(f.Geometry.Bound().Min, f.Geometry.Bound().Max, f)
		delete(s.features, id)
		msg.cb <- nil
		if s.leader {
			s.replicateAction(msg.action, f)
		}
	case "checkpoint":
		msg.cb <- s.makeCheckpoint()
	default:
		panic("unrecognised operation")
	}
}

func (s *Storage) Run() {
	if s.leader {
		s.init()
	} else {
		_, err := os.Create(s.name + ".wal")
		if err != nil {
			panic(err)
		}
	}
	go func() {
		for {
			select {
			case <-s.ctx.Done():
				return
			case msg := <-s.queue:
				s.handleMessage(msg)
			}
		}
	}()
}

func (s *Storage) Stop() {
	s.cancel()
	for _, conn := range s.replicas {
		conn.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
		conn.Close()
	}
}

func main() {
	mux := http.NewServeMux()
	shard := Shard{"primary", []string{"replica"}}
	r := NewRouter(mux, []Shard{shard})
	replicaStorage := NewStorage(mux, "replica", false)
	primaryStorage := NewStorage(mux, "primary", true)
	server := http.Server{}
	server.Addr = "127.0.0.1:8080"
	r.url = "127.0.0.1:8080"
	server.Handler = mux

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		slog.Info("Listen http://" + server.Addr)
		err := server.ListenAndServe()
		if !errors.Is(err, http.ErrServerClosed) {
			slog.Info("err", "err", err)
		}
		wg.Done()
	}()
	time.Sleep(5 * time.Second)
	primaryStorage.appendReplica(server.Addr, "replica")

	r.Run()
	defer r.Stop()
	replicaStorage.Run()
	defer replicaStorage.Stop()
	primaryStorage.Run()
	defer primaryStorage.Stop()

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
	wg.Wait()
}
