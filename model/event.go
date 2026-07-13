package model

type LogEvent struct {
	ServerName string `json:"serverName"`
	Path       string `json:"path"`
	Channel    string `json:"channel"`
	Timestamp  string `json:"timestamp"`
	Message    string `json:"message"`
}
