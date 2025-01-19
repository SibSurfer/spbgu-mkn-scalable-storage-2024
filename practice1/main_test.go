package main

import (
	"bytes"
	"math/rand/v2"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/paulmach/orb"
	"github.com/paulmach/orb/geojson"
	"github.com/stretchr/testify/assert"
)

func TestSimple(t *testing.T) {
	mux := http.NewServeMux()

	s := NewStorage(mux, "test", []string{}, true)
	go func() { s.Run() }()

	r := NewRouter(mux, [][]string{{"test"}})
	go func() { r.Run() }()

	t.Cleanup(r.Stop)
	t.Cleanup(s.Stop)
	point := geojson.NewFeature(orb.Point{rand.Float64(), rand.Float64()})
	point.ID = "1"

	postRequest(t, mux, "/insert", point)
	collection := sendSelect(t, mux)
	assert.Equal(t, 1, len(collection.Features))
	assert.Equal(t, point.Geometry, collection.Features[0].Geometry)

	point2 := geojson.NewFeature(orb.Point{rand.Float64(), rand.Float64()})
	point2.ID = "1"

	postRequest(t, mux, "/replace", point2)
	collection = sendSelect(t, mux)
	assert.Equal(t, 1, len(collection.Features))
	assert.Equal(t, point2.Geometry, collection.Features[0].Geometry)

	postRequest(t, mux, "/delete", point2)
	collection = sendSelect(t, mux)
	assert.Equal(t, 0, len(collection.Features))
}

func postRequest(t *testing.T, mux *http.ServeMux, url string, feature *geojson.Feature) {
	body, err := feature.MarshalJSON()
	if err != nil {
		t.Fatal(err)
	}

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

func sendSelect(t *testing.T, mux *http.ServeMux) *geojson.FeatureCollection {
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
