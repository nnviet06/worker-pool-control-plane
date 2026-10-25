package main

import (
	"fmt"
	"strings"
)

type workerAddr struct {
	id, addr string
}

// workersFlag is the static worker list, set as "a=host:port,b=host:port".
type workersFlag []workerAddr

func (f *workersFlag) String() string {
	parts := make([]string, len(*f))
	for i, w := range *f {
		parts[i] = w.id + "=" + w.addr
	}
	return strings.Join(parts, ",")
}

func (f *workersFlag) Set(s string) error {
	seen := make(map[string]bool, len(*f))
	for _, w := range *f {
		seen[w.id] = true
	}
	for part := range strings.SplitSeq(s, ",") {
		id, addr, ok := strings.Cut(part, "=")
		if !ok || id == "" || addr == "" {
			return fmt.Errorf("want id=host:port, got %q", part)
		}
		if seen[id] {
			return fmt.Errorf("worker %s listed twice", id)
		}
		seen[id] = true
		*f = append(*f, workerAddr{id, addr})
	}
	return nil
}
