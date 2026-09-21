package containers

import "encoding/json"

// Entry is the part of one container list item the agent uses. The list
// carries much more — command line, labels, env-derived ports — and none of
// it is decoded, so nothing like a password on a command line can leak into
// an event.
type Entry struct {
	ID    string   `json:"Id"`
	Names []string `json:"Names"`
	Image string   `json:"Image"`
	State string   `json:"State"`
}

// Parse decodes Docker's container list.
func Parse(data []byte) ([]Entry, error) {
	var entries []Entry
	if err := json.Unmarshal(data, &entries); err != nil {
		return nil, err
	}
	return entries, nil
}
