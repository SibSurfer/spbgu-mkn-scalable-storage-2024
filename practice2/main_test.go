package main

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"testing"

	"github.com/paulmach/orb"
	"github.com/paulmach/orb/geojson"
	"github.com/stretchr/testify/assert"
)

func postRequest(t *testing.T, mux *http.ServeMux, url string, body []byte) {
	req, err := http.NewRequest("POST", url, bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)
	if rr.Code != http.StatusTemporaryRedirect {
		t.Fatalf("handler returned wrong status code: got %v want %v", rr.Code, http.StatusTemporaryRedirect)
	}

	req, err = http.NewRequest("POST", rr.Header().Get("location"), bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	rr = httptest.NewRecorder()
	mux.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("handler returned wrong status code: got %v want %v", rr.Code, http.StatusOK)
	}
}

func insertRequest(t *testing.T, mux *http.ServeMux, feature *geojson.Feature) {
	body, err := feature.MarshalJSON()
	if err != nil {
		t.Fatal(err)
	}
	postRequest(t, mux, "/insert", body)
}

func replaceRequest(t *testing.T, mux *http.ServeMux, feature *geojson.Feature) {
	body, err := feature.MarshalJSON()
	if err != nil {
		t.Fatal(err)
	}
	postRequest(t, mux, "/replace", body)
}

func deleteRequest(t *testing.T, mux *http.ServeMux, feature *geojson.Feature) {
	body, err := feature.MarshalJSON()
	if err != nil {
		t.Fatal(err)
	}
	postRequest(t, mux, "/delete", body)
}

func checkpointRequest(t *testing.T, mux *http.ServeMux) {
	postRequest(t, mux, "/checkpoint", []byte{})
}

func selectRequest(t *testing.T, mux *http.ServeMux) *geojson.FeatureCollection {
	body := make([]byte, 0)
	req, err := http.NewRequest("GET", "/select", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)
	if rr.Code != http.StatusTemporaryRedirect {
		t.Fatalf("handler returned wrong status code: got %v want %v", rr.Code, http.StatusTemporaryRedirect)
	}

	req, err = http.NewRequest("GET", rr.Header().Get("location"), bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	rr = httptest.NewRecorder()
	mux.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("handler returned wrong status code: got %v want %v", rr.Code, http.StatusOK)
	}
	collection, err := geojson.UnmarshalFeatureCollection(rr.Body.Bytes())
	if err != nil {
		t.Fatal(err)
	}
	return collection
}

func Test(t *testing.T) {
	point := geojson.NewFeature(orb.Point{3, 3})
	point.ID = "1"

	point2 := geojson.NewFeature(orb.Point{1, 1})
	point2.ID = "1"

	polygon := geojson.NewFeature(orb.Polygon{{{1, 1}, {2, 2}, {3, 3}, {4, 4}}})
	polygon.ID = "2"

	t.Cleanup(func() {
		os.Remove("test.wal")
	})
	t.Cleanup(func() {
		os.Remove("test.ckp")
	})
	t.Run("insert-replace", func(t *testing.T) {
		mux := http.NewServeMux()

		s := NewStorage(mux, "test", []string{}, true)
		s.Run()

		r := NewRouter(mux, [][]string{{"test"}})
		r.Run()

		t.Cleanup(r.Stop)
		t.Cleanup(s.Stop)

		insertRequest(t, mux, point)
		collection := selectRequest(t, mux)
		assert.Equal(t, 1, len(collection.Features))
		assert.Equal(t, point.Geometry, collection.Features[0].Geometry)

		replaceRequest(t, mux, point2)
		collection = selectRequest(t, mux)
		assert.Equal(t, 1, len(collection.Features))
		assert.Equal(t, point2.Geometry, collection.Features[0].Geometry)
	})

	t.Run("delete", func(t *testing.T) {
		mux := http.NewServeMux()

		s := NewStorage(mux, "test", []string{}, true)
		s.Run()

		r := NewRouter(mux, [][]string{{"test"}})
		r.Run()

		t.Cleanup(r.Stop)
		t.Cleanup(s.Stop)

		collection := selectRequest(t, mux)
		assert.Equal(t, 1, len(collection.Features))
		assert.Equal(t, point2.Geometry, collection.Features[0].Geometry)

		deleteRequest(t, mux, point2)
		collection = selectRequest(t, mux)
		assert.Equal(t, 0, len(collection.Features))
	})

	t.Run("checkpoint1", func(t *testing.T) {
		mux := http.NewServeMux()

		s := NewStorage(mux, "test", []string{}, true)
		s.Run()

		r := NewRouter(mux, [][]string{{"test"}})
		r.Run()

		t.Cleanup(r.Stop)
		t.Cleanup(s.Stop)

		insertRequest(t, mux, polygon)
		checkpointRequest(t, mux)
		insertRequest(t, mux, point)
		deleteRequest(t, mux, point)
	})

	t.Run("checkpoint2", func(t *testing.T) {
		mux := http.NewServeMux()

		s := NewStorage(mux, "test", []string{}, true)
		s.Run()

		r := NewRouter(mux, [][]string{{"test"}})
		r.Run()

		t.Cleanup(r.Stop)
		t.Cleanup(s.Stop)

		collection := selectRequest(t, mux)
		assert.Equal(t, 1, len(collection.Features))
		assert.Equal(t, polygon.Geometry, collection.Features[0].Geometry)
	})
	t.Run("concurrent-operations", func(t *testing.T) {
		mux := http.NewServeMux()

		s := NewStorage(mux, "test", []string{}, true)
		s.Run()

		r := NewRouter(mux, [][]string{{"test"}})
		r.Run()

		t.Cleanup(r.Stop)
		t.Cleanup(s.Stop)

		var wg sync.WaitGroup
		wg.Add(2)

		go func() {
			defer wg.Done()
			insertRequest(t, mux, point)
		}()

		go func() {
			defer wg.Done()
			insertRequest(t, mux, point2)
		}()

		wg.Wait()

		collection := selectRequest(t, mux)
		assert.Equal(t, 2, len(collection.Features))
	})

}
