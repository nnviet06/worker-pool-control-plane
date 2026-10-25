package limiter

import "testing"

func TestStaticRaisesToOne(t *testing.T) {
	if got := NewStatic(0).Limit(); got != 1 {
		t.Fatalf("NewStatic(0).Limit() = %d, want 1", got)
	}
	if got := NewStatic(4).Limit(); got != 4 {
		t.Fatalf("NewStatic(4).Limit() = %d, want 4", got)
	}
}

func TestEffective(t *testing.T) {
	tests := []struct {
		name                          string
		limit, liveWorkers, liveSlots int
		want                          int
	}{
		{"within range", 3, 2, 4, 3},
		{"above live slots", 4, 1, 2, 2},
		{"below live workers", 1, 2, 4, 2},
		{"no live worker", 4, 0, 0, 0},
	}
	for _, tt := range tests {
		if got := Effective(tt.limit, tt.liveWorkers, tt.liveSlots); got != tt.want {
			t.Errorf("%s: Effective(%d, %d, %d) = %d, want %d",
				tt.name, tt.limit, tt.liveWorkers, tt.liveSlots, got, tt.want)
		}
	}
}
