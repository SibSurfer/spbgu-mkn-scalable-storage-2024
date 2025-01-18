package main

import (
	"slices"
	"testing"
)

func TestFiller(t *testing.T) {
	b := [100]byte{}
	zero := byte('0')
	one := byte('1')
	filler(b[:], zero, one)
	// Заполнить здесь ассерт, что b содержит zero и что b содержит one

	slice := b[:]

	if !slices.Contains(slice, zero) {
		t.Errorf("Expected '0' in b")
	}

	if !slices.Contains(slice, one) {
		t.Errorf("Expected '1' in b")
	}
}
