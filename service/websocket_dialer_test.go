package service

import (
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/QuantumNous/new-api/common"

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConfigureWebSocketDialerProxyModes(t *testing.T) {
	t.Run("http", func(t *testing.T) {
		dialer := *websocket.DefaultDialer
		require.NoError(t, ConfigureWebSocketDialer(&dialer, "http://proxy.example:8080", nil))
		require.NotNil(t, dialer.Proxy)

		proxyURL, err := dialer.Proxy(&http.Request{URL: &url.URL{Scheme: "https", Host: "api.example"}})
		require.NoError(t, err)
		require.NotNil(t, proxyURL)
		assert.Equal(t, "http://proxy.example:8080", proxyURL.String())
	})

	for _, scheme := range []string{"socks5", "socks5h"} {
		t.Run(scheme, func(t *testing.T) {
			dialer := *websocket.DefaultDialer
			require.NoError(t, ConfigureWebSocketDialer(&dialer, scheme+"://proxy.example:1080", nil))
			assert.Nil(t, dialer.Proxy)
			assert.NotNil(t, dialer.NetDialContext)
		})
	}

	t.Run("https", func(t *testing.T) {
		dialer := *websocket.DefaultDialer
		require.NoError(t, ConfigureWebSocketDialer(&dialer, "https://proxy.example:8443", nil))
		assert.Nil(t, dialer.Proxy)
		assert.NotNil(t, dialer.NetDialContext)
	})

	t.Run("invalid", func(t *testing.T) {
		dialer := *websocket.DefaultDialer
		require.Error(t, ConfigureWebSocketDialer(&dialer, "ftp://proxy.example", nil))
	})
}

func TestResponsesWSEnvironmentHTTPSProxy(t *testing.T) {
	old := common.TLSInsecureSkipVerify
	common.TLSInsecureSkipVerify = true
	t.Cleanup(func() { common.TLSInsecureSkipVerify = old })
	upgradeWaiting := make(chan struct{})
	releaseUpgrade := make(chan struct{})
	upstream := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("stall") == "1" {
			close(upgradeWaiting)
			select {
			case <-r.Context().Done():
			case <-releaseUpgrade:
			}
			return
		}
		ws, err := (&websocket.Upgrader{}).Upgrade(w, r, nil)
		if !assert.NoError(t, err) {
			return
		}
		defer ws.Close()
		messageType, payload, err := ws.ReadMessage()
		if err == nil {
			assert.NoError(t, ws.WriteMessage(messageType, payload))
		}
	}))
	defer upstream.Close()
	proxy := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !assert.Equal(t, http.MethodConnect, r.Method) {
			return
		}
		dest, err := net.Dial("tcp", strings.TrimPrefix(upstream.URL, "https://"))
		if !assert.NoError(t, err) {
			return
		}
		defer dest.Close()
		client, _, err := w.(http.Hijacker).Hijack()
		if !assert.NoError(t, err) {
			return
		}
		defer client.Close()
		_, err = io.WriteString(client, "HTTP/1.1 200 Connection Established\r\n\r\n")
		if !assert.NoError(t, err) {
			return
		}
		done := make(chan struct{})
		go func() {
			defer close(done)
			_, _ = io.Copy(dest, client)
			_ = dest.Close()
		}()
		_, _ = io.Copy(client, dest)
		_ = client.Close()
		<-done
	}))
	defer proxy.Close()
	defer close(releaseUpgrade)
	t.Setenv("HTTPS_PROXY", proxy.URL)
	t.Setenv("https_proxy", proxy.URL)
	t.Setenv("NO_PROXY", "")
	t.Setenv("no_proxy", "")
	for _, explicit := range []bool{true, false} {
		t.Run(map[bool]string{true: "channel", false: "environment"}[explicit], func(t *testing.T) {
			dialer := *websocket.DefaultDialer
			raw := ""
			if explicit {
				raw = proxy.URL
			}
			require.NoError(t, ConfigureWebSocketDialer(&dialer, raw, &url.URL{Scheme: "wss", Host: "upstream.invalid"}))
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			conn, _, err := DialWebSocketContext(ctx, &dialer, "wss://upstream.invalid/v1/responses", nil)
			require.NoError(t, err, "environment and channel HTTPS proxy must both work")
			defer conn.Close()
			cancel()
			require.NoError(t, conn.SetReadDeadline(time.Now().Add(3*time.Second)))
			require.NoError(t, conn.WriteMessage(websocket.TextMessage, []byte("after handoff")))
			_, payload, err := conn.ReadMessage()
			require.NoError(t, err, "dial context cancellation must not close an established connection")
			assert.Equal(t, "after handoff", string(payload))
		})
	}
	t.Run("cancel_after_connect", func(t *testing.T) {
		dialer := *websocket.DefaultDialer
		require.NoError(t, ConfigureWebSocketDialer(&dialer, proxy.URL, &url.URL{Scheme: "wss", Host: "upstream.invalid"}))
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		done := make(chan error, 1)
		go func() {
			conn, _, err := DialWebSocketContext(ctx, &dialer, "wss://upstream.invalid/v1/responses?stall=1", nil)
			if conn != nil {
				_ = conn.Close()
			}
			done <- err
		}()
		select {
		case <-upgradeWaiting:
		case err := <-done:
			t.Fatalf("dial failed before Upgrade: %v", err)
		case <-time.After(3 * time.Second):
			t.Fatal("Upgrade request not received through HTTPS tunnel")
		}
		cancel()
		select {
		case err := <-done:
			require.ErrorIs(t, err, context.Canceled)
		case <-time.After(time.Second):
			t.Fatal("cancellation did not interrupt Upgrade after CONNECT succeeded")
		}
	})

}

func TestResponsesWSEnvironmentProxySelection(t *testing.T) {
	for _, tc := range []struct {
		name, scheme, noProxy string
		wantProxy, wantDial   bool
	}{
		{name: "HTTP", scheme: "http", wantProxy: true},
		{name: "SOCKS remote DNS", scheme: "socks5h", wantDial: true},
		{name: "HTTPS bypass", scheme: "https", noProxy: "api.example"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("HTTPS_PROXY", tc.scheme+"://proxy.example:8443")
			t.Setenv("NO_PROXY", tc.noProxy)
			t.Setenv("no_proxy", tc.noProxy)
			dialer := *websocket.DefaultDialer
			require.NoError(t, ConfigureWebSocketDialer(&dialer, "", &url.URL{Scheme: "wss", Host: "api.example"}))
			assert.Equal(t, tc.wantProxy, dialer.Proxy != nil)
			assert.Equal(t, tc.wantDial, dialer.NetDialContext != nil)
		})
	}
}

func TestResponsesWSHandshakeCancellation(t *testing.T) {
	for _, mode := range []string{"direct_upgrade", "direct_tls_upgrade", "http_proxy_connect", "https_proxy_connect"} {
		t.Run(mode, func(t *testing.T) {
			old := common.TLSInsecureSkipVerify
			common.TLSInsecureSkipVerify = true
			t.Cleanup(func() { common.TLSInsecureSkipVerify = old })
			received := make(chan struct{})
			release := make(chan struct{})
			handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if strings.HasSuffix(mode, "proxy_connect") {
					assert.Equal(t, http.MethodConnect, r.Method)
				} else {
					assert.Equal(t, "websocket", r.Header.Get("Upgrade"))
				}
				close(received)
				<-release
				w.WriteHeader(http.StatusBadGateway)
			})
			var server *httptest.Server
			if mode == "direct_tls_upgrade" || mode == "https_proxy_connect" {
				server = httptest.NewTLSServer(handler)
			} else {
				server = httptest.NewServer(handler)
			}
			defer server.Close()
			t.Setenv("HTTP_PROXY", "")
			t.Setenv("http_proxy", "")
			t.Setenv("HTTPS_PROXY", "")
			t.Setenv("https_proxy", "")
			dialer := *websocket.DefaultDialer
			dialer.HandshakeTimeout = 5 * time.Second
			target := "ws" + strings.TrimPrefix(server.URL, "http") + "/v1/responses"
			proxy := ""
			if strings.HasSuffix(mode, "proxy_connect") {
				proxy = server.URL
				target = "wss://upstream.invalid/v1/responses"
			}
			targetURL, err := url.Parse(target)
			require.NoError(t, err)
			require.NoError(t, ConfigureWebSocketDialer(&dialer, proxy, targetURL))
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := make(chan error, 1)
			go func() {
				conn, _, err := DialWebSocketContext(ctx, &dialer, target, nil)
				if conn != nil {
					_ = conn.Close()
				}
				done <- err
			}()
			select {
			case <-received:
			case err := <-done:
				close(release)
				t.Fatalf("dial failed before handshake: %v", err)
			case <-time.After(3 * time.Second):
				close(release)
				t.Fatal("handshake not received")
			}
			cancel()
			select {
			case err := <-done:
				close(release)
				require.ErrorIs(t, err, context.Canceled)
			case <-time.After(time.Second):
				t.Error("context cancelled after handshake request, but dial still blocked")
				close(release)
				select {
				case err := <-done:
					require.ErrorIs(t, err, context.Canceled)
				case <-time.After(3 * time.Second):
					t.Fatal("dial did not exit after proxy/server release")
				}
			}
		})
	}
}
