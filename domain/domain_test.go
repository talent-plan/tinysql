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
// See the License for the specific language governing permissions and
// limitations under the License.

package domain

import (
	"github.com/ngaut/pools"
	. "github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/pingcap/parser/mysql"
	"testing"
)

func TestT(t *testing.T) {
	CustomVerboseFlag = true
	TestingT(t)
}

var _ = Suite(&testSuite{})

type testSuite struct {
}

func mockFactory() (pools.Resource, error) {
	return nil, errors.New("mock factory should not be called")
}

func sysMockFactory(dom *Domain) (pools.Resource, error) {
	return nil, nil
}

type testResource struct {
	status int
}

func (tr *testResource) Close() { tr.status = 1 }

func (*testSuite) TestSessionPool(c *C) {
	f := func() (pools.Resource, error) { return &testResource{}, nil }
	pool := newSessionPool(1, f)
	tr, err := pool.Get()
	c.Assert(err, IsNil)
	tr1, err := pool.Get()
	c.Assert(err, IsNil)
	pool.Put(tr)
	// Capacity is 1, so tr1 is closed.
	pool.Put(tr1)
	c.Assert(tr1.(*testResource).status, Equals, 1)
	pool.Close()

	pool.Close()
	pool.Put(tr1)
	tr, err = pool.Get()
	c.Assert(err.Error(), Equals, "session pool closed")
	c.Assert(tr, IsNil)
}

func (*testSuite) TestErrorCode(c *C) {
	c.Assert(int(ErrInfoSchemaExpired.ToSQLError().Code), Equals, mysql.ErrInfoSchemaExpired)
	c.Assert(int(ErrInfoSchemaChanged.ToSQLError().Code), Equals, mysql.ErrInfoSchemaChanged)
}
