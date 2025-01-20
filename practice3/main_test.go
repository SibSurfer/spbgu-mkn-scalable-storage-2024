package main

import (
	"bytes"
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

func selectRequest(t *testing.T, url string) *geojson.FeatureCollection {
	resp, err := http.Get(url + "/select")
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

func Test1(t *testing.T) {
	point := geojson.NewFeature(orb.Point{3, 3})
	point.ID = "1"

	point2 := geojson.NewFeature(orb.Point{1, 1})
	point2.ID = "1"

	polygon := geojson.NewFeature(orb.Polygon{{{1, 1}, {2, 2}, {3, 3}, {4, 4}}})
	polygon.ID = "2"

	t.Cleanup(func() {
		os.Remove("test.wal")
		os.Remove("test.ckp")
	})
	t.Run("replace", func(t *testing.T) {
		mux := http.NewServeMux()
		s := NewStorage(mux, "test", true)
		s.Run()

		r := NewRouter(mux, [][]string{{"test"}})
		r.Run()

		t.Cleanup(r.Stop)
		t.Cleanup(s.Stop)

		server := httptest.NewServer(mux)
		defer server.Close()
		url := server.URL

		insert(t, url, point)
		collection := selectRequest(t, url)
		assert.Equal(t, 1, len(collection.Features))
		assert.Equal(t, point.Geometry, collection.Features[0].Geometry)

		replace(t, url, point2)
		collection = selectRequest(t, url)
		assert.Equal(t, 1, len(collection.Features))
		assert.Equal(t, point2.Geometry, collection.Features[0].Geometry)
	})

	t.Run("delete", func(t *testing.T) {
		mux := http.NewServeMux()
		s := NewStorage(mux, "test", true)
		s.Run()

		r := NewRouter(mux, [][]string{{"test"}})
		r.Run()

		t.Cleanup(r.Stop)
		t.Cleanup(s.Stop)

		server := httptest.NewServer(mux)
		defer server.Close()
		url := server.URL

		collection := selectRequest(t, url)
		assert.Equal(t, 1, len(collection.Features))
		assert.Equal(t, point2.Geometry, collection.Features[0].Geometry)

		deleteRequest(t, url, point2)
		collection = selectRequest(t, url)
		assert.Equal(t, 0, len(collection.Features))
	})

	t.Run("checkpoint1", func(t *testing.T) {
		mux := http.NewServeMux()
		s := NewStorage(mux, "test", true)
		s.Run()

		r := NewRouter(mux, [][]string{{"test"}})
		r.Run()

		t.Cleanup(r.Stop)
		t.Cleanup(s.Stop)

		server := httptest.NewServer(mux)
		defer server.Close()
		url := server.URL

		insert(t, url, polygon)
		checkpoint(t, url)
		insert(t, url, point)
		deleteRequest(t, url, point)
	})

	t.Run("checkpoint2", func(t *testing.T) {
		mux := http.NewServeMux()
		s := NewStorage(mux, "test", true)
		s.Run()

		r := NewRouter(mux, [][]string{{"test"}})
		r.Run()

		t.Cleanup(r.Stop)
		t.Cleanup(s.Stop)

		server := httptest.NewServer(mux)
		defer server.Close()
		url := server.URL

		collection := selectRequest(t, url)
		assert.Equal(t, 1, len(collection.Features))
		assert.Equal(t, polygon.Geometry, collection.Features[0].Geometry)
	})
}

func TestReplica(t *testing.T) {
	point := geojson.NewFeature(orb.Point{1, 1})
	point.ID = "1"
	line := geojson.NewFeature(orb.LineString{{2, 2}, {3, 3}})
	line.ID = "2"
	polygon := geojson.NewFeature(orb.Polygon{{{0, 0}, {3, 0}, {0, 4}, {0, 0}}})
	polygon.ID = "3"

	mux := http.NewServeMux()
	primaryStorage := NewStorage(mux, "primary", true)
	primaryStorage.Run()

	replicaStorage := NewStorage(mux, "replica", false)
	replicaStorage.Run()

	r := NewRouter(mux, [][]string{{"primary", "replica"}})
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

	insert(t, url, point)
	insert(t, url, line)
	insert(t, url, polygon)

	primaryStorage.appendReplica(url, "replica")

	time.Sleep(1 * time.Second)

	for i := 1; i < 100; i++ {
		collection := selectRequest(t, url)
		assert.Equal(t, 3, len(collection.Features))
	}

	deleteRequest(t, url, point)
	deleteRequest(t, url, line)
	deleteRequest(t, url, polygon)

	time.Sleep(1 * time.Second)

	for i := 1; i < 100; i++ {
		collection := selectRequest(t, url)
		assert.Equal(t, 0, len(collection.Features))
	}
}
