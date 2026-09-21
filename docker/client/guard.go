package client

import (
	"fmt"
	"net/http"
	"regexp"
)

// allowed is every request the agent may send to Docker, matched against the
// full path and query. All are reads: docker info, docker ps -a,
// docker stats --no-stream and docker inspect. Container ids must be the full
// 64-hex form the list returns, so no name or path trick can slip through.
var allowed = []*regexp.Regexp{
	regexp.MustCompile(`^/info$`),
	regexp.MustCompile(`^/containers/json\?all=true$`),
	regexp.MustCompile(`^/containers/[0-9a-f]{64}/stats\?stream=false&one-shot=true$`),
	regexp.MustCompile(`^/containers/[0-9a-f]{64}/json$`),
}

// readOnlyTransport refuses any request that is not a GET on the allow-list.
// A refused request never reaches the socket.
type readOnlyTransport struct{ next http.RoundTripper }

func (t readOnlyTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if req.Method != http.MethodGet {
		return nil, fmt.Errorf("docker: refused %s %s: read-only client", req.Method, req.URL.RequestURI())
	}
	target := req.URL.RequestURI()
	for _, re := range allowed {
		if re.MatchString(target) {
			return t.next.RoundTrip(req)
		}
	}
	return nil, fmt.Errorf("docker: refused GET %s: not on the allow-list", target)
}
