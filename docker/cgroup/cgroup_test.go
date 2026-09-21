package cgroup

import "testing"

func TestParse(t *testing.T) {
	cases := []struct {
		name string
		info string
		want int
	}{
		{"v2 host", `{"ServerVersion":"29.6.1","CgroupDriver":"cgroupfs","CgroupVersion":"2"}`, 2},
		{"v1 host", `{"ServerVersion":"20.10.24","CgroupDriver":"systemd","CgroupVersion":"1"}`, 1},
		{"docker before 20.10", `{"ServerVersion":"19.03.15","CgroupDriver":"cgroupfs"}`, 1},
	}
	for _, tc := range cases {
		got, err := Parse([]byte(tc.info))
		if err != nil {
			t.Fatalf("%s: unexpected error %v", tc.name, err)
		}
		if got != tc.want {
			t.Fatalf("%s: expected %d, got %d", tc.name, tc.want, got)
		}
	}
}

func TestParse_UnknownVersion(t *testing.T) {
	if _, err := Parse([]byte(`{"CgroupVersion":"3"}`)); err == nil {
		t.Fatal("expected an error for an unknown cgroup version")
	}
}

func TestParse_Invalid(t *testing.T) {
	if _, err := Parse([]byte(`not json`)); err == nil {
		t.Fatal("expected an error for a reply that is not json")
	}
}
