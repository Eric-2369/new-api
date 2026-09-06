package service

import (
	"bufio"
	"context"
	"crypto/tls"
	"encoding/base64"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"time"

	"github.com/QuantumNous/new-api/common"

	"github.com/gorilla/websocket"
	"golang.org/x/net/http/httpproxy"
)

// DialWebSocketContext keeps cancellation active until the complete WebSocket
// handshake finishes, including proxy CONNECT and the HTTP Upgrade response.
// Gorilla only applies a deadline to those reads, so DialContext alone does not
// interrupt them when the caller cancels without reaching its deadline.
func DialWebSocketContext(ctx context.Context, dialer *websocket.Dialer, target string, headers http.Header) (*websocket.Conn, *http.Response, error) {
	if dialer == nil {
		dialer = websocket.DefaultDialer
	}
	configured := *dialer
	// Use our own context: Gorilla cancels its internal handshake context even
	// on success, before returning the established connection to this function.
	if configured.HandshakeTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, configured.HandshakeTimeout)
		defer cancel()
	}
	var stopCancel func() bool
	trackConnection := func(dial func(context.Context, string, string) (net.Conn, error)) func(context.Context, string, string) (net.Conn, error) {
		return func(dialCtx context.Context, network, address string) (net.Conn, error) {
			connection, err := dial(dialCtx, network, address)
			if err != nil {
				return nil, err
			}
			// Close the transport directly; a TLS close_notify can wait for a blocked
			// writer, which is precisely what cancellation must interrupt.
			transport := connection
			for {
				tlsConnection, ok := transport.(*tls.Conn)
				if !ok {
					break
				}
				transport = tlsConnection.NetConn()
			}
			stopCancel = context.AfterFunc(ctx, func() { _ = transport.Close() })
			return connection, nil
		}
	}
	dial := configured.NetDialContext
	if dial == nil {
		if configured.NetDial != nil {
			dial = func(_ context.Context, network, address string) (net.Conn, error) {
				return configured.NetDial(network, address)
			}
		} else {
			dial = (&net.Dialer{}).DialContext
		}
	}
	configured.NetDialContext = trackConnection(dial)
	if configured.NetDialTLSContext != nil {
		configured.NetDialTLSContext = trackConnection(configured.NetDialTLSContext)
	}
	connection, response, err := configured.DialContext(ctx, target, headers)
	// Once a cancellation callback has started, the socket cannot be handed off
	// as healthy even if the Upgrade response arrived concurrently.
	stopped := true
	if stopCancel != nil {
		stopped = stopCancel()
	}
	if !stopped || ctx.Err() != nil {
		if connection != nil {
			_ = connection.Close()
		}
		return nil, response, ctx.Err()
	}
	return connection, response, err
}

// ConfigureWebSocketDialer applies the same outbound proxy and TLS trust
// settings used by relay HTTP clients to a WebSocket dialer.
func ConfigureWebSocketDialer(dialer *websocket.Dialer, rawProxyURL string, targetURL *url.URL) error {
	if dialer == nil {
		return fmt.Errorf("websocket dialer is nil")
	}
	if common.TLSInsecureSkipVerify {
		dialer.TLSClientConfig = common.InsecureTLSConfig.Clone()
	}

	parsedURL, legacySuffixStripped, err := common.ParseProxyURLRuntime(rawProxyURL)
	if err != nil {
		return err
	}
	if parsedURL == nil {
		if targetURL == nil {
			return fmt.Errorf("websocket target URL is required for environment proxy selection")
		}
		target := *targetURL
		switch target.Scheme {
		case "ws":
			target.Scheme = "http"
		case "wss":
			target.Scheme = "https"
		}
		parsedURL, err = httpproxy.FromEnvironment().ProxyFunc()(&target)
		if err != nil {
			return err
		}
		if parsedURL == nil {
			dialer.Proxy = nil
			return nil
		}
	}
	config := newProxyURLConfig(parsedURL)
	if legacySuffixStripped {
		warnLegacyProxyURLOnce(config)
	}

	switch parsedURL.Scheme {
	case "http":
		dialer.Proxy = http.ProxyURL(parsedURL)
		return nil
	case "socks5", "socks5h":
		transport := newRelayHTTPTransport()
		if err = configureProxyTransport(transport, parsedURL); err != nil {
			return err
		}
		dialer.Proxy = nil
		dialer.NetDialContext = transport.DialContext
		return nil
	case "https":
		proxyAddress := parsedURL.Host
		if parsedURL.Port() == "" {
			proxyAddress = net.JoinHostPort(parsedURL.Hostname(), "443")
		}
		proxyTLSConfig := &tls.Config{ServerName: parsedURL.Hostname()}
		if common.TLSInsecureSkipVerify {
			proxyTLSConfig = common.InsecureTLSConfig.Clone()
			proxyTLSConfig.ServerName = parsedURL.Hostname()
		}
		forwardDialer := &net.Dialer{Timeout: 30 * time.Second, KeepAlive: 30 * time.Second}
		dialer.Proxy = nil
		dialer.NetDialContext = func(ctx context.Context, network, address string) (net.Conn, error) {
			connection, dialErr := forwardDialer.DialContext(ctx, network, proxyAddress)
			if dialErr != nil {
				return nil, dialErr
			}
			proxyConnection := tls.Client(connection, proxyTLSConfig.Clone())
			if handshakeErr := proxyConnection.HandshakeContext(ctx); handshakeErr != nil {
				_ = connection.Close()
				return nil, handshakeErr
			}

			// CONNECT reads and writes do not accept a context. Close their
			// socket on cancellation, and disarm the callback before handoff.
			stopCancel := context.AfterFunc(ctx, func() { _ = connection.Close() })
			defer stopCancel()

			deadline := time.Now().Add(dialer.HandshakeTimeout)
			if dialer.HandshakeTimeout <= 0 {
				deadline = time.Now().Add(45 * time.Second)
			}
			if contextDeadline, ok := ctx.Deadline(); ok && contextDeadline.Before(deadline) {
				deadline = contextDeadline
			}
			_ = proxyConnection.SetDeadline(deadline)
			connectHeader := http.Header{}
			if user := parsedURL.User; user != nil {
				password, _ := user.Password()
				credentials := base64.StdEncoding.EncodeToString([]byte(user.Username() + ":" + password))
				connectHeader.Set("Proxy-Authorization", "Basic "+credentials)
			}
			connectRequest := &http.Request{
				Method: http.MethodConnect,
				URL:    &url.URL{Opaque: address},
				Host:   address,
				Header: connectHeader,
			}
			if writeErr := connectRequest.Write(proxyConnection); writeErr != nil {
				_ = proxyConnection.Close()
				return nil, writeErr
			}
			response, readErr := http.ReadResponse(bufio.NewReader(proxyConnection), connectRequest)
			if readErr != nil {
				_ = proxyConnection.Close()
				return nil, readErr
			}
			if response.StatusCode != http.StatusOK {
				_ = proxyConnection.Close()
				return nil, fmt.Errorf("HTTPS proxy CONNECT failed: %s", response.Status)
			}
			if !stopCancel() {
				_ = proxyConnection.Close()
				return nil, ctx.Err()
			}
			if err := ctx.Err(); err != nil {
				_ = proxyConnection.Close()
				return nil, err
			}
			_ = proxyConnection.SetDeadline(time.Time{})
			return proxyConnection, nil
		}
		return nil
	default:
		return fmt.Errorf("unsupported proxy scheme")
	}
}
