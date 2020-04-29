// Copyright 2017 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package server

import (
	"bytes"
	"fmt"
	"go.uber.org/zap"
	"net"
	"net/http"
	"net/http/pprof"

	"github.com/gorilla/mux"
	"github.com/pingcap/tidb/util"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/soheilhy/cmux"
)

const defaultStatusPort = 10080

func (s *Server) startStatusHTTP() {
	go s.startHTTPServer()
}

func (s *Server) startHTTPServer() {
	router := mux.NewRouter()

	addr := fmt.Sprintf("%s:%d", s.cfg.Status.StatusHost, s.cfg.Status.StatusPort)
	if s.cfg.Status.StatusPort == 0 {
		addr = fmt.Sprintf("%s:%d", s.cfg.Status.StatusHost, defaultStatusPort)
	}

	serverMux := http.NewServeMux()
	serverMux.Handle("/", router)

	serverMux.HandleFunc("/debug/pprof/", pprof.Index)
	serverMux.HandleFunc("/debug/pprof/cmdline", pprof.Cmdline)
	serverMux.HandleFunc("/debug/pprof/profile", pprof.Profile)
	serverMux.HandleFunc("/debug/pprof/symbol", pprof.Symbol)
	serverMux.HandleFunc("/debug/pprof/trace", pprof.Trace)

	var (
		httpRouterPage bytes.Buffer
		pathTemplate   string
		err            error
	)

	err = router.Walk(func(route *mux.Route, router *mux.Router, ancestors []*mux.Route) error {
		pathTemplate, err = route.GetPathTemplate()
		if err != nil {
			logutil.BgLogger().Error("get HTTP router path failed", zap.Error(err))
		}
		name := route.GetName()
		// If the name attribute is not set, GetName returns "".
		if name != "" {
			httpRouterPage.WriteString("<tr><td><a href='" + pathTemplate + "'>" + name + "</a><td></tr>")
		}
		return nil
	})
	if err != nil {
		logutil.BgLogger().Error("generate root failed", zap.Error(err))
	}
	httpRouterPage.WriteString("<tr><td><a href='/debug/pprof/'>Debug</a><td></tr>")
	httpRouterPage.WriteString("</table></body></html>")
	router.HandleFunc("/", func(responseWriter http.ResponseWriter, request *http.Request) {
		_, err = responseWriter.Write([]byte(httpRouterPage.String()))
		if err != nil {
			logutil.BgLogger().Error("write HTTP index page failed", zap.Error(err))
		}
	})

	s.setupStatusServer(addr, serverMux)
}

func (s *Server) setupStatusServer(addr string, serverMux *http.ServeMux) {
	l, err := net.Listen("tcp", addr)
	if err != nil {
		logutil.BgLogger().Info("listen failed", zap.Error(err))
		return
	}
	m := cmux.New(l)
	// Match connections in order:
	// First HTTP, and otherwise grpc.
	httpL := m.Match(cmux.HTTP1Fast())

	s.statusServer = &http.Server{Addr: addr, Handler: CorsHandler{handler: serverMux, cfg: s.cfg}}

	go util.WithRecovery(func() {
		err := s.statusServer.Serve(httpL)
		logutil.BgLogger().Error("http server error", zap.Error(err))
	}, nil)
	err = m.Serve()
	if err != nil {
		logutil.BgLogger().Error("start status/rpc server error", zap.Error(err))
	}
}
