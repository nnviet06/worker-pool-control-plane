package main

import (
	"fmt"
	"sort"
	"strings"
	"time"
)

// latencyFlag maps a class to its service time, set as
// "default=200ms,prefill=400ms". Classes without an entry use "default".
type latencyFlag map[string]time.Duration

func (l latencyFlag) of(class string) time.Duration {
	if d, ok := l[class]; ok {
		return d
	}
	return l["default"]
}

func (l latencyFlag) String() string {
	parts := make([]string, 0, len(l))
	for c, d := range l {
		parts = append(parts, c+"="+d.String())
	}
	sort.Strings(parts)
	return strings.Join(parts, ",")
}

func (l latencyFlag) Set(s string) error {
	for part := range strings.SplitSeq(s, ",") {
		class, value, ok := strings.Cut(part, "=")
		if !ok || class == "" {
			return fmt.Errorf("want class=duration, got %q", part)
		}
		d, err := time.ParseDuration(value)
		if err != nil {
			return fmt.Errorf("class %s: %w", class, err)
		}
		l[class] = d
	}
	return nil
}
