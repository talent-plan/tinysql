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
	"context"
	"crypto/tls"
	"testing"
	"time"

	"github.com/ngaut/pools"
	. "github.com/pingcap/check"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/model"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/ddl"
	"github.com/pingcap/tidb/domain/infosync"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/store/mockstore"
	"github.com/pingcap/tidb/util/mock"
	"github.com/pingcap/tidb/util/testleak"
	"go.etcd.io/etcd/integration"
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

type mockEtcdBackend struct {
	kv.Storage
	pdAddrs []string
}

func (mebd *mockEtcdBackend) EtcdAddrs() []string {
	return mebd.pdAddrs
}
func (mebd *mockEtcdBackend) TLSConfig() *tls.Config { return nil }
func (mebd *mockEtcdBackend) StartGCWorker() error {
	panic("not implemented")
}

func TestInfo(t *testing.T) {
	defer testleak.AfterTestT(t)()
	ddlLease := 80 * time.Millisecond
	s, err := mockstore.NewMockTikvStore()
	if err != nil {
		t.Fatal(err)
	}
	clus := integration.NewClusterV3(t, &integration.ClusterConfig{Size: 1})
	defer clus.Terminate(t)
	mockStore := &mockEtcdBackend{
		Storage: s,
		pdAddrs: []string{clus.Members[0].GRPCAddr()}}
	dom := NewDomain(mockStore, ddlLease, 0, mockFactory)
	defer func() {
		dom.Close()
		s.Close()
	}()

	cli := clus.RandClient()
	dom.etcdClient = cli
	// Mock new DDL and init the schema syncer with etcd client.
	goCtx := context.Background()
	dom.ddl = ddl.NewDDL(
		goCtx,
		ddl.WithEtcdClient(dom.GetEtcdClient()),
		ddl.WithStore(s),
		ddl.WithInfoHandle(dom.infoHandle),
		ddl.WithLease(ddlLease),
	)
	err = failpoint.Enable("github.com/pingcap/tidb/domain/MockReplaceDDL", `return(true)`)
	if err != nil {
		t.Fatal(err)
	}
	err = dom.Init(ddlLease, sysMockFactory)
	if err != nil {
		t.Fatal(err)
	}
	err = failpoint.Disable("github.com/pingcap/tidb/domain/MockReplaceDDL")
	if err != nil {
		t.Fatal(err)
	}

	// Test for GetServerInfo and GetServerInfoByID.
	ddlID := dom.ddl.GetID()
	serverInfo, err := infosync.GetServerInfo()
	if err != nil {
		t.Fatal(err)
	}
	info, err := infosync.GetServerInfoByID(goCtx, ddlID)
	if err != nil {
		t.Fatal(err)
	}
	if serverInfo.ID != info.ID {
		t.Fatalf("server self info %v, info %v", serverInfo, info)
	}
	_, err = infosync.GetServerInfoByID(goCtx, "not_exist_id")
	if err == nil || (err != nil && err.Error() != "[info-syncer] get /tidb/server/info/not_exist_id failed") {
		t.Fatal(err)
	}

	// Test for GetAllServerInfo.
	infos, err := infosync.GetAllServerInfo(goCtx)
	if err != nil {
		t.Fatal(err)
	}
	if len(infos) != 1 || infos[ddlID].ID != info.ID {
		t.Fatalf("server one info %v, info %v", infos[ddlID], info)
	}

	// Test the scene where syncer.Done() gets the information.
	err = failpoint.Enable("github.com/pingcap/tidb/ddl/util/ErrorMockSessionDone", `return(true)`)
	if err != nil {
		t.Fatal(err)
	}
	<-dom.ddl.SchemaSyncer().Done()
	time.Sleep(15 * time.Millisecond)
	syncerStarted := false
	for i := 0; i < 200; i++ {
		if dom.SchemaValidator.IsStarted() {
			syncerStarted = true
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if !syncerStarted {
		t.Fatal("start syncer failed")
	}
	err = failpoint.Disable("github.com/pingcap/tidb/ddl/util/ErrorMockSessionDone")
	if err != nil {
		t.Fatal(err)
	}
	// Make sure loading schema is normal.
	cs := &ast.CharsetOpt{
		Chs: "utf8",
		Col: "utf8_bin",
	}
	ctx := mock.NewContext()
	err = dom.ddl.CreateSchema(ctx, model.NewCIStr("aaa"), cs)
	if err != nil {
		t.Fatal(err)
	}
	err = dom.Reload()
	if err != nil {
		t.Fatal(err)
	}
	if dom.InfoSchema().SchemaMetaVersion() != 1 {
		t.Fatalf("update schema version failed, ver %d", dom.InfoSchema().SchemaMetaVersion())
	}

	// Test for RemoveServerInfo.
	dom.info.RemoveServerInfo()
	infos, err = infosync.GetAllServerInfo(goCtx)
	if err != nil || len(infos) != 0 {
		t.Fatalf("err %v, infos %v", err, infos)
	}
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
