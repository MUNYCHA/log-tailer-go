package load

import "testing"

func TestParse(t *testing.T) {
	got, err := Parse([]byte("0.02 0.04 0.05 1/791 12163\n"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	one, five, fifteen := Averages(got)
	if one != 0.02 || five != 0.04 || fifteen != 0.05 {
		t.Fatalf("expected 0.02/0.04/0.05, got %v/%v/%v", one, five, fifteen)
	}
}

func TestParse_TooFewFields(t *testing.T) {
	if _, err := Parse([]byte("0.02 0.04\n")); err == nil {
		t.Fatal("expected error for a truncated loadavg line, got nil")
	}
}

func TestParse_NotANumber(t *testing.T) {
	if _, err := Parse([]byte("x y z 1/791 12163\n")); err == nil {
		t.Fatal("expected error for non-numeric loadavg, got nil")
	}
}
