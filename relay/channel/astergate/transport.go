// Package astergate implements the local AsterGate extension. Keep wire handling
// here so rebasing the host only requires the channel registration and dispatch hooks.
package astergate

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strings"
)

// Endpoint accepts an instance root or its /v1 API root, including a deployment prefix.
func Endpoint(base, path string) (string, error) {
	u, err := url.Parse(strings.TrimSpace(base))
	if err != nil || u == nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return "", errors.New("invalid AsterGate base URL")
	}
	u.Path = strings.TrimSuffix(strings.TrimRight(u.Path, "/"), "/v1") + "/v1/" + strings.TrimLeft(path, "/")
	u.RawPath = ""
	return u.String(), nil
}

// Headers strips transport headers and credentials in both directions. Inbound
// identity and forwarding headers must never impersonate a peer or a local user.
func Headers(src http.Header) http.Header {
	dst := src.Clone()
	if dst == nil {
		dst = make(http.Header)
	}
	for _, value := range src.Values("Connection") {
		for key := range strings.SplitSeq(value, ",") {
			dst.Del(strings.TrimSpace(key))
		}
	}
	for key := range dst {
		lower := strings.ToLower(key)
		switch lower {
		case "connection", "proxy-connection", "keep-alive", "proxy-authenticate", "proxy-authorization", "te", "trailer", "transfer-encoding", "upgrade", "authorization", "x-api-key", "cookie", "set-cookie", "host", "content-length", "accept-encoding", "forwarded", "x-real-ip", "new-api-user", "x-management-key":
			dst.Del(key)
		default:
			if strings.HasPrefix(lower, "x-forwarded-") || strings.HasPrefix(lower, "x-astergate-") || strings.HasPrefix(lower, "x-new-api-") {
				dst.Del(key)
			}
		}
	}
	return dst
}

func Request(ctx context.Context, client *http.Client, base, path, query, key string, headers http.Header, body io.Reader) (*http.Response, error) {
	endpoint, err := Endpoint(base, path)
	if err != nil {
		return nil, err
	}
	if strings.TrimSpace(key) == "" {
		return nil, errors.New("missing AsterGate API key")
	}
	if query != "" {
		endpoint += "?" + query
	}
	method := http.MethodPost
	if path == "usage" {
		method = http.MethodGet
	}
	req, err := http.NewRequestWithContext(ctx, method, endpoint, body)
	if err != nil {
		return nil, err
	}
	req.Header = Headers(headers)
	req.Header.Set("Authorization", "Bearer "+strings.TrimSpace(key))
	req.Header.Set("Accept-Encoding", "identity")
	if path == "usage" {
		req.Header.Set("X-Astergate-Usage-Protocol", "1")
	}
	// Do not mutate the shared client or forward credentials through a redirect.
	isolated := *client
	isolated.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	return isolated.Do(req)
}
