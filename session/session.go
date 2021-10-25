// Copyright 2015 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Copyright 2013 The ql Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSES/QL-LICENSE file.

package session

import (
	"tinysql/sessionctx"
	"tinysql/sessionctx/variable"
)

// Session context, it is consistent with the lifecycle of a client connection.
type Session interface {
	sessionctx.Context
	Status() uint16 // Flag of current status, such as autocommit.
	Close()
}

type session struct {
	sessionVars *variable.SessionVars
}

// GetSessionVars implements the context.Context interface.
func (s *session) GetSessionVars() *variable.SessionVars {
	return s.sessionVars
}

// CreateSession creates a new session environment.
func CreateSession() (Session, error) {
	s := &session{
		sessionVars: variable.NewSessionVars(),
	}
	return s, nil
}

func (s *session) Close() {
}

func (s *session) Status() uint16 {
	return s.sessionVars.Status
}
