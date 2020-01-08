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

package config

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"

	. "github.com/pingcap/check"
)

var _ = Suite(&testConfigSuite{})

type testConfigSuite struct{}

func TestT(t *testing.T) {
	CustomVerboseFlag = true
	TestingT(t)
}

func (s *testConfigSuite) TestConfig(c *C) {
	conf := new(Config)
	configFile := "config.toml"
	_, localFile, _, _ := runtime.Caller(0)
	configFile = filepath.Join(filepath.Dir(localFile), configFile)

	f, err := os.Create(configFile)
	c.Assert(err, IsNil)

	// Make sure the server refuses to start if there's an unrecognized configuration option
	_, err = f.WriteString(`
unrecognized-option-test = true
`)
	c.Assert(err, IsNil)
	c.Assert(f.Sync(), IsNil)

	c.Assert(conf.Load(configFile), ErrorMatches, "(?:.|\n)*unknown configuration option(?:.|\n)*")

	f.Truncate(0)
	f.Seek(0, 0)

	_, err = f.WriteString(`
token-limit = 0
`)

	c.Assert(err, IsNil)
	c.Assert(f.Sync(), IsNil)

	c.Assert(conf.Load(configFile), IsNil)

	c.Assert(conf.TokenLimit, Equals, uint(1000))
	c.Assert(f.Close(), IsNil)
	c.Assert(os.Remove(configFile), IsNil)

	configFile = filepath.Join(filepath.Dir(localFile), "config.toml.example")
	c.Assert(conf.Load(configFile), IsNil)

	// Make sure the example config is the same as default config.
	c.Assert(conf, DeepEquals, GetGlobalConfig())
}
