package containers

import (
	"sort"
	"strings"
)

// Container is one container as published.
type Container struct {
	ID    string
	Name  string
	Image string
	State string
}

// ToContainers turns list entries into published containers, sorted by name
// so the same server lists its containers in the same order every tick.
// Docker prefixes names with "/" (a relic of container links); it is dropped
// so the name matches what docker ps shows.
func ToContainers(entries []Entry) []Container {
	out := make([]Container, len(entries))
	for i, e := range entries {
		var name string
		if len(e.Names) > 0 {
			name = strings.TrimPrefix(e.Names[0], "/")
		}
		out[i] = Container{ID: e.ID, Name: name, Image: e.Image, State: e.State}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Name != out[j].Name {
			return out[i].Name < out[j].Name
		}
		return out[i].ID < out[j].ID
	})
	return out
}
