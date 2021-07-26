package allocrunner

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/hashicorp/go-cleanhttp"
	hclog "github.com/hashicorp/go-hclog"
	"github.com/hashicorp/nomad/client/allocdir"
	"github.com/hashicorp/nomad/client/allocrunner/interfaces"
	"github.com/hashicorp/nomad/nomad/structs"
	"github.com/hashicorp/nomad/nomad/structs/config"
	"github.com/pkg/errors"
)

func tgFirstNetworkIsBridge(tg *structs.TaskGroup) bool {
	if len(tg.Networks) < 1 || tg.Networks[0].Mode != "bridge" {
		return false
	}
	return true
}

const (
	consulHTTPSocketHookName = "consul_http_socket"
)

type consulHTTPSockHook struct {
	logger hclog.Logger

	// lock synchronizes proxy and alloc which may be mutated and read concurrently
	// via Prerun, Update, and Postrun.
	lock  sync.Mutex
	alloc *structs.Allocation
	proxy *httpSocketProxy
}

func newConsulHTTPSocketHook(logger hclog.Logger, alloc *structs.Allocation, allocDir *allocdir.AllocDir, config *config.ConsulConfig) *consulHTTPSockHook {
	return &consulHTTPSockHook{
		alloc:  alloc,
		proxy:  newHTTPSocketProxy(logger, allocDir, config),
		logger: logger.Named(consulHTTPSocketHookName),
	}
}

func (*consulHTTPSockHook) Name() string {
	return consulHTTPSocketHookName
}

// shouldRun returns true if the alloc contains at least one connect native
// task and has a network configured in bridge mode
//
// todo(shoenig): what about CNI networks?
func (h *consulHTTPSockHook) shouldRun() bool {
	tg := h.alloc.Job.LookupTaskGroup(h.alloc.TaskGroup)

	// we must be in bridge networking and at least one connect native task
	if !tgFirstNetworkIsBridge(tg) {
		return false
	}

	for _, service := range tg.Services {
		if service.Connect.IsNative() {
			return true
		}
	}
	return false
}

func (h *consulHTTPSockHook) Prerun() error {
	h.lock.Lock()
	defer h.lock.Unlock()

	if !h.shouldRun() {
		return nil
	}

	return h.proxy.run(h.alloc)
}

func (h *consulHTTPSockHook) Update(req *interfaces.RunnerUpdateRequest) error {
	h.lock.Lock()
	defer h.lock.Unlock()

	h.alloc = req.Alloc

	if !h.shouldRun() {
		return nil
	}

	return h.proxy.run(h.alloc)
}

func (h *consulHTTPSockHook) Postrun() error {
	h.lock.Lock()
	defer h.lock.Unlock()

	if err := h.proxy.stop(); err != nil {
		// Only log a failure to stop, worst case is the proxy leaks a goroutine.
		h.logger.Warn("error stopping Consul HTTP proxy", "error", err)
	}

	return nil
}

type httpSocketProxy struct {
	logger   hclog.Logger
	allocDir *allocdir.AllocDir
	config   *config.ConsulConfig

	ctx     context.Context
	cancel  func()
	doneCh  chan struct{}
	runOnce bool
}

func newHTTPSocketProxy(logger hclog.Logger, allocDir *allocdir.AllocDir, config *config.ConsulConfig) *httpSocketProxy {
	ctx, cancel := context.WithCancel(context.Background())
	return &httpSocketProxy{
		logger:   logger,
		allocDir: allocDir,
		config:   config,
		ctx:      ctx,
		cancel:   cancel,
		doneCh:   make(chan struct{}),
	}
}

// run the httpSocketProxy for the given allocation.
//
// Assumes locking done by the calling alloc runner.
func (p *httpSocketProxy) run(alloc *structs.Allocation) error {
	// Only run once.
	if p.runOnce {
		return nil
	}

	// Never restart.
	select {
	case <-p.doneCh:
		p.logger.Trace("consul http socket proxy already shutdown; exiting")
		return nil
	case <-p.ctx.Done():
		p.logger.Trace("consul http socket proxy already done; exiting")
		return nil
	default:
	}

	// consul http dest addr
	destAddr := p.config.Addr
	if destAddr == "" {
		return errors.New("consul address must be set on nomad client")
	}

	hostHTTPSockPath := filepath.Join(p.allocDir.AllocDir, allocdir.AllocHTTPSocket)
	if err := maybeRemoveOldSocket(hostHTTPSockPath); err != nil {
		return err
	}

	listener, err := net.Listen("unix", hostHTTPSockPath)
	if err != nil {
		return errors.Wrap(err, "unable to create unix socket for Consul HTTP endpoint")
	}

	// The Consul HTTP socket should be usable by all users in case a task is
	// running as a non-privileged user. Unix does not allow setting domain
	// socket permissions when creating the file, so we must manually call
	// chmod afterwards.
	if err := os.Chmod(hostHTTPSockPath, os.ModePerm); err != nil {
		return errors.Wrap(err, "unable to set permissions on unix socket")
	}

	go func() {
		proxyHTTP(p.ctx, p.logger, destAddr, listener)
		p.cancel()
		close(p.doneCh)
	}()

	p.runOnce = true
	return nil
}

func forward(r *http.Request, host string) *http.Request {
	var hop = []string{
		"Connection",
		"Keep-Alive",
		"Proxy-Authenticate",
		"Proxy-Authorization",
		"Te",
		"Trailers",
		"Transfer-Encoding",
		"Upgrade",
	}

	r2 := r.Clone(context.Background())

	// remove this thing from first request
	r2.RequestURI = ""

	// set url for forward address

	// environment variable?

	r2.URL.Scheme = "http" // what if https?
	r2.URL.Host = host

	// remove headers set during first request
	for _, header := range hop {
		r2.Header.Del(header)
	}

	// append forward header chain
	if prior := r2.Header.Get("X-Forwarded-For"); prior != "" {
		host = prior + ", " + host
	}
	r2.Header.Set("X-Forwarded-For", host)

	// return our copy
	return r2
}

func proxyHTTP(ctx context.Context, logger hclog.Logger, destAddr string, l net.Listener) {
	httpClient := cleanhttp.DefaultClient()

	if err := (&http.Server{
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			r2 := forward(r, destAddr)
			fmt.Println("r2:", r2)

			resp, err := httpClient.Do(r2)
			if err != nil {
				fmt.Println("do error:", err)
				return
			}
			defer resp.Body.Close()

			for k, v := range resp.Header {
				w.Header()[k] = v
			}
			w.WriteHeader(resp.StatusCode)
			_, _ = io.Copy(w, resp.Body)
		}),
	}).Serve(l); err != nil {
		fmt.Println("pH serve err:", err)
	}
}

func (p *httpSocketProxy) stop() error {
	p.cancel()

	// if proxy was never run, no need to wait before shutdown
	if !p.runOnce {
		return nil
	}

	select {
	case <-p.doneCh:
	case <-time.After(socketProxyStopWaitTime):
		return errSocketProxyTimeout
	}

	return nil
}

func maybeRemoveOldSocket(socketPath string) error {
	_, err := os.Stat(socketPath)
	if err == nil {
		if err = os.Remove(socketPath); err != nil {
			return errors.Wrap(err, "unable to remove existing unix socket")
		}
	}
	return nil
}
