package main

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/paulmach/orb"
	"github.com/paulmach/orb/geojson"
	"github.com/stretchr/testify/assert"
)

func postRequest(t *testing.T, url string, body []byte) {
	resp, err := http.Post(url, "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("handler returned wrong status code: got %v want %v", resp.StatusCode, http.StatusOK)
	}
}

func insert(t *testing.T, url string, feature *geojson.Feature) {
	body, err := feature.MarshalJSON()
	if err != nil {
		t.Fatal(err)
	}
	postRequest(t, url+"/insert", body)
}

func replace(t *testing.T, url string, feature *geojson.Feature) {
	body, err := feature.MarshalJSON()
	if err != nil {
		t.Fatal(err)
	}
	postRequest(t, url+"/replace", body)
}

func deleteRequest(t *testing.T, url string, feature *geojson.Feature) {
	body, err := feature.MarshalJSON()
	if err != nil {
		t.Fatal(err)
	}
	postRequest(t, url+"/delete", body)
}

func checkpoint(t *testing.T, url string) {
	postRequest(t, url+"/checkpoint", []byte{})
}

func selectRequest(t *testing.T, url string, min, max [2]float64) *geojson.FeatureCollection {
	resp, err := http.Get(url + "/select?rect=" + fmt.Sprint(min[0]) + "," + fmt.Sprint(min[1]) + "," + fmt.Sprint(max[0]) + "," + fmt.Sprint(max[1]))
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("handler returned wrong status code: got %v want %v", resp.StatusCode, http.StatusOK)
	}
	json, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	collection, err := geojson.UnmarshalFeatureCollection(json)
	if err != nil {
		t.Fatal(err)
	}
	return collection
}

func Test(t *testing.T) {
	point := geojson.NewFeature(orb.Point{1, 1})
	point.ID = "1"

	line := geojson.NewFeature(orb.LineString{{1, 1}, {2, 2}})
	line.ID = "1"

	polygon := geojson.NewFeature(orb.Polygon{{{1, 1}, {2, 2}, {3, 3}, {4, 4}}})
	polygon.ID = "2"

	min := [2]float64{0.1, 0.1}
	max := [2]float64{5.0, 5.0}

	t.Cleanup(func() {
		os.Remove("test.wal")
		os.Remove("test.ckp")
	})
	t.Run("replace", func(t *testing.T) {
		mux := http.NewServeMux()
		s := NewStorage(mux, "test", true)
		s.Run()

		r := NewRouter(mux, []Shard{{"test", []string{}}})
		r.Run()

		t.Cleanup(r.Stop)
		t.Cleanup(s.Stop)

		server := httptest.NewServer(mux)
		defer server.Close()
		url := server.URL
		r.url = url

		insert(t, url, point)
		collection := selectRequest(t, url, min, max)
		assert.Equal(t, 1, len(collection.Features))
		assert.Equal(t, point.Geometry, collection.Features[0].Geometry)

		replace(t, url, line)
		collection = selectRequest(t, url, min, max)
		assert.Equal(t, 1, len(collection.Features))
		assert.Equal(t, line.Geometry, collection.Features[0].Geometry)
	})

	t.Run("delete", func(t *testing.T) {
		mux := http.NewServeMux()
		s := NewStorage(mux, "test", true)
		s.Run()
		r := NewRouter(mux, []Shard{{"test", []string{}}})
		r.Run()
		t.Cleanup(r.Stop)
		t.Cleanup(s.Stop)

		server := httptest.NewServer(mux)
		defer server.Close()
		url := server.URL
		r.url = url

		collection := selectRequest(t, url, min, max)
		assert.Equal(t, 1, len(collection.Features))
		assert.Equal(t, line.Geometry, collection.Features[0].Geometry)

		deleteRequest(t, url, line)
		collection = selectRequest(t, url, min, max)
		assert.Equal(t, 0, len(collection.Features))
	})

	t.Run("checkpoint1", func(t *testing.T) {
		mux := http.NewServeMux()
		s := NewStorage(mux, "test", true)
		s.Run()

		r := NewRouter(mux, []Shard{{"test", []string{}}})
		r.Run()

		t.Cleanup(r.Stop)
		t.Cleanup(s.Stop)

		server := httptest.NewServer(mux)
		defer server.Close()
		url := server.URL
		r.url = url

		insert(t, url, polygon)
		checkpoint(t, url)
		insert(t, url, point)
		deleteRequest(t, url, point)
	})

	t.Run("checkpoint2", func(t *testing.T) {
		mux := http.NewServeMux()
		s := NewStorage(mux, "test", true)
		s.Run()

		r := NewRouter(mux, []Shard{{"test", []string{}}})
		r.Run()

		t.Cleanup(r.Stop)
		t.Cleanup(s.Stop)

		server := httptest.NewServer(mux)
		defer server.Close()
		url := server.URL
		r.url = url

		collection := selectRequest(t, url, min, max)
		assert.Equal(t, 1, len(collection.Features))
		assert.Equal(t, polygon.Geometry, collection.Features[0].Geometry)
	})
}

func TestReplica(t *testing.T) {
	point := geojson.NewFeature(orb.Point{1, 1})
	point.ID = "1"

	line := geojson.NewFeature(orb.LineString{{1, 1}, {2, 2}})
	line.ID = "2"

	polygon := geojson.NewFeature(orb.Polygon{{{1, 1}, {2, 2}, {3, 3}, {4, 4}}})
	polygon.ID = "3"

	min := [2]float64{0.5, 0.5}
	max := [2]float64{5.5, 5.5}

	mux := http.NewServeMux()
	primaryStorage := NewStorage(mux, "primary", true)
	primaryStorage.Run()

	replicaStorage := NewStorage(mux, "replica", false)
	replicaStorage.Run()

	r := NewRouter(mux, []Shard{{"primary", []string{"replica"}}})
	r.Run()

	t.Cleanup(func() {
		r.Stop()
		replicaStorage.Stop()
		primaryStorage.Stop()
		os.Remove("primary.wal")
		os.Remove("replica.wal")
		os.Remove("primary.ckp")
		os.Remove("replica.ckp")
	})

	server := httptest.NewServer(mux)
	defer server.Close()
	url := server.URL
	r.url = url

	insert(t, url, point)
	insert(t, url, line)
	insert(t, url, polygon)

	primaryStorage.appendReplica(url, "replica")

	time.Sleep(1 * time.Second)

	for i := 1; i < 100; i++ {
		collection := selectRequest(t, url, min, max)
		assert.Equal(t, 3, len(collection.Features))
	}

	deleteRequest(t, url, point)
	deleteRequest(t, url, line)
	deleteRequest(t, url, polygon)

	time.Sleep(1 * time.Second)

	for i := 1; i < 100; i++ {
		collection := selectRequest(t, url, min, max)
		assert.Equal(t, 0, len(collection.Features))
	}
}

func TestSharding(t *testing.T) {
	point1 := geojson.NewFeature(orb.Point{1, 1})
	point1.ID = "1"
	point2 := geojson.NewFeature(orb.Point{-1, -1})
	point2.ID = "2"
	point3 := geojson.NewFeature(orb.Point{4, 5})
	point3.ID = "3"
	point4 := geojson.NewFeature(orb.Point{0, 0})
	point4.ID = "4"

	mux := http.NewServeMux()
	primaryStor1 := NewStorage(mux, "primary1", true)
	primaryStor1.Run()
	replicaStor1 := NewStorage(mux, "replica1", false)
	replicaStor1.Run()
	primaryStor2 := NewStorage(mux, "primary2", true)
	primaryStor2.Run()
	replicaStor2 := NewStorage(mux, "replica2", false)
	replicaStor2.Run()

	r := NewRouter(mux, []Shard{{"primary1", []string{"replica1"}}, {"primary2", []string{"replica2"}}})
	r.Run()

	t.Cleanup(func() {
		r.Stop()
		replicaStor1.Stop()
		primaryStor1.Stop()
		replicaStor2.Stop()
		primaryStor2.Stop()
		os.Remove("primary1.wal")
		os.Remove("replica1.wal")
		os.Remove("primary1.ckp")
		os.Remove("replica1.ckp")
		os.Remove("primary2.wal")
		os.Remove("replica2.wal")
		os.Remove("primary2.ckp")
		os.Remove("replica2.ckp")
	})

	server := httptest.NewServer(mux)
	defer server.Close()
	url := server.URL
	r.url = url

	insert(t, url, point1)
	insert(t, url, point2)
	insert(t, url, point3)
	insert(t, url, point4)

	primaryStor1.appendReplica(url, "replica1")
	primaryStor2.appendReplica(url, "replica2")

	time.Sleep(1 * time.Second)

	for i := 1; i < 50; i++ {
		collection := selectRequest(t, url, [2]float64{0.1, 0.1}, [2]float64{5.0, 5.0})
		assert.Equal(t, 2, len(collection.Features))
		collection = selectRequest(t, url, [2]float64{-5.0, 0.1}, [2]float64{-0.1, 5.0})
		assert.Equal(t, 2, len(collection.Features))
	}

	deleteRequest(t, url, point1)
	deleteRequest(t, url, point3)

	time.Sleep(1 * time.Second)

	for i := 1; i < 50; i++ {
		collection := selectRequest(t, url, [2]float64{0.1, 0.1}, [2]float64{5.0, 5.0})
		assert.Equal(t, 0, len(collection.Features))
		collection = selectRequest(t, url, [2]float64{-5.0, 0.1}, [2]float64{-0.1, 5.0})
		assert.Equal(t, 2, len(collection.Features))
	}
}
