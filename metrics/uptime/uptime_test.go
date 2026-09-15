package uptime

import "testing"

func TestParseAndSeconds_TruncatesToWholeSeconds(t *testing.T) {
	sample, err := Parse([]byte("12345.67 54321.00\n"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if sample.Seconds != 12345.67 {
		t.Fatalf("expected raw 12345.67, got %f", sample.Seconds)
	}
	if got := Seconds(sample); got != 12345 {
		t.Fatalf("expected 12345, got %d", got)
	}
}

func TestParse_Empty(t *testing.T) {
	if _, err := Parse([]byte("")); err == nil {
		t.Fatal("expected error for empty input, got nil")
	}
}

func TestParse_NotANumber(t *testing.T) {
	if _, err := Parse([]byte("not-a-number 0\n")); err == nil {
		t.Fatal("expected error for non-numeric input, got nil")
	}
}
