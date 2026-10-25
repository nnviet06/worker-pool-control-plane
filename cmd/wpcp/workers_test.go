package main

import (
	"slices"
	"testing"
)

func TestWorkersFlag(t *testing.T) {
	var f workersFlag
	if err := f.Set("a=127.0.0.1:50051,b=127.0.0.1:50052"); err != nil {
		t.Fatal(err)
	}
	want := workersFlag{{"a", "127.0.0.1:50051"}, {"b", "127.0.0.1:50052"}}
	if !slices.Equal(f, want) {
		t.Fatalf("parsed %v", f)
	}
	for _, bad := range []string{"a", "=host:1", "a=", "a=h:1,a=h:2"} {
		var f workersFlag
		if err := f.Set(bad); err == nil {
			t.Errorf("Set(%q) accepted", bad)
		}
	}
}
