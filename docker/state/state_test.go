package state

import "testing"

// Trimmed from a real Docker 29.6 reply (mysql-mysql-1), with a Config block
// holding a secret that must never be decoded
const sampleExited = `{
  "Id": "fa5eb4ae064f7908a3a6ed36514e5c97b1d56e05274daccbe6d84cfbb51ec338",
  "State": {
    "Status": "exited",
    "Running": false,
    "OOMKilled": false,
    "Pid": 0,
    "ExitCode": 255,
    "StartedAt": "2026-09-10T02:43:21.818732981Z",
    "FinishedAt": "2026-09-11T02:39:19.015766807Z"
  },
  "Config": {"Env": ["MYSQL_ROOT_PASSWORD=secret"]}
}`

// Created but never started: Docker reports exit code 0 and a zero time
const sampleCreated = `{
  "State": {"Status": "created", "Running": false, "ExitCode": 0,
    "StartedAt": "0001-01-01T00:00:00Z", "FinishedAt": "0001-01-01T00:00:00Z"}
}`

// Started again between the container list and this request: running, with
// the previous run's exit code and finish time still set
const sampleRestarted = `{
  "State": {"Status": "running", "Running": true, "ExitCode": 137,
    "StartedAt": "2026-09-21T08:00:05Z", "FinishedAt": "2026-09-21T08:00:01Z"}
}`

func parse(t *testing.T, data string) Sample {
	t.Helper()
	s, err := Parse([]byte(data))
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func TestExit_Exited(t *testing.T) {
	got, ok := Exit(parse(t, sampleExited))
	want := LastExit{ExitCode: 255, FinishedAt: "2026-09-11T02:39:19Z"}
	if !ok || got != want {
		t.Fatalf("expected %+v, got %+v (ok=%v)", want, got, ok)
	}
}

// A clean exit is a real 0, published, not omitted
func TestExit_CleanExitIsZero(t *testing.T) {
	got, ok := Exit(Sample{ExitCode: 0, FinishedAt: parse(t, sampleExited).FinishedAt})
	if !ok || got.ExitCode != 0 {
		t.Fatalf("expected exit code 0 published, got %+v (ok=%v)", got, ok)
	}
}

func TestExit_NeverStartedOmitted(t *testing.T) {
	if got, ok := Exit(parse(t, sampleCreated)); ok {
		t.Fatalf("expected omitted for a never-started container, got %+v", got)
	}
}

func TestExit_StartedAgainOmitted(t *testing.T) {
	if got, ok := Exit(parse(t, sampleRestarted)); ok {
		t.Fatalf("expected omitted for a container running again, got %+v", got)
	}
}

func TestParse_Invalid(t *testing.T) {
	if _, err := Parse([]byte(`not json`)); err == nil {
		t.Fatal("expected an error for a reply that is not json")
	}
}
