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
	"encoding/json"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/BurntSushi/toml"
	. "github.com/pingcap/check"
	zaplog "github.com/pingcap/log"
	"github.com/pingcap/parser/mysql"
	"github.com/pingcap/tidb/util/logutil"
)

var _ = Suite(&testConfigSuite{})

type testConfigSuite struct{}

func TestT(t *testing.T) {
	CustomVerboseFlag = true
	TestingT(t)
}

func (s *testConfigSuite) TestNullableBoolUnmashal(c *C) {
	var nb = nullableBool{false, false}
	data, err := json.Marshal(nb)
	c.Assert(err, IsNil)
	err = json.Unmarshal(data, &nb)
	c.Assert(err, IsNil)
	c.Assert(nb, Equals, nbUnset)

	nb = nullableBool{true, false}
	data, err = json.Marshal(nb)
	c.Assert(err, IsNil)
	err = json.Unmarshal(data, &nb)
	c.Assert(err, IsNil)
	c.Assert(nb, Equals, nbFalse)

	nb = nullableBool{true, true}
	data, err = json.Marshal(nb)
	c.Assert(err, IsNil)
	err = json.Unmarshal(data, &nb)
	c.Assert(err, IsNil)
	c.Assert(nb, Equals, nbTrue)

	// Test for UnmarshalText
	var log Log
	_, err = toml.Decode("enable-error-stack = true", &log)
	c.Assert(err, IsNil)
	c.Assert(log.EnableErrorStack, Equals, nbTrue)

	_, err = toml.Decode("enable-error-stack = \"\"", &log)
	c.Assert(err, IsNil)
	c.Assert(log.EnableErrorStack, Equals, nbUnset)

	_, err = toml.Decode("enable-error-stack = 1", &log)
	c.Assert(err, ErrorMatches, "Invalid value for bool type: 1")
	c.Assert(log.EnableErrorStack, Equals, nbUnset)

	// Test for UnmarshalJSON
	err = json.Unmarshal([]byte("{\"enable-timestamp\":false}"), &log)
	c.Assert(err, IsNil)
	c.Assert(log.EnableTimestamp, Equals, nbFalse)

	err = json.Unmarshal([]byte("{\"disable-timestamp\":null}"), &log)
	c.Assert(err, IsNil)
	c.Assert(log.DisableTimestamp, Equals, nbUnset)
}

func (s *testConfigSuite) TestLogConfig(c *C) {
	var conf Config
	configFile := "log_config.toml"
	_, localFile, _, _ := runtime.Caller(0)
	configFile = filepath.Join(filepath.Dir(localFile), configFile)

	f, err := os.Create(configFile)
	c.Assert(err, IsNil)
	defer func() {
		c.Assert(f.Close(), IsNil)
		c.Assert(os.Remove(configFile), IsNil)
	}()

	var testLoad = func(confStr string, expectedEnableErrorStack, expectedDisableErrorStack, expectedEnableTimestamp, expectedDisableTimestamp nullableBool, resultedDisableTimestamp, resultedDisableErrorVerbose bool, valid Checker) {
		conf = defaultConf
		_, err = f.WriteString(confStr)
		c.Assert(err, IsNil)
		c.Assert(conf.Load(configFile), IsNil)
		c.Assert(conf.Valid(), valid)
		c.Assert(conf.Log.EnableErrorStack, Equals, expectedEnableErrorStack)
		c.Assert(conf.Log.DisableErrorStack, Equals, expectedDisableErrorStack)
		c.Assert(conf.Log.EnableTimestamp, Equals, expectedEnableTimestamp)
		c.Assert(conf.Log.DisableTimestamp, Equals, expectedDisableTimestamp)
		c.Assert(conf.Log.ToLogConfig(), DeepEquals, logutil.NewLogConfig("info", "text", conf.Log.File, resultedDisableTimestamp, func(config *zaplog.Config) { config.DisableErrorVerbose = resultedDisableErrorVerbose }))
		f.Truncate(0)
		f.Seek(0, 0)
	}

	testLoad(`
[Log]
`, nbUnset, nbUnset, nbUnset, nbUnset, false, true, IsNil)

	testLoad(`
[Log]
enable-timestamp = false
`, nbUnset, nbUnset, nbFalse, nbUnset, true, true, IsNil)

	testLoad(`
[Log]
enable-timestamp = true
disable-timestamp = false
`, nbUnset, nbUnset, nbTrue, nbFalse, false, true, IsNil)

	testLoad(`
[Log]
enable-timestamp = false
disable-timestamp = true
`, nbUnset, nbUnset, nbFalse, nbTrue, true, true, IsNil)

	testLoad(`
[Log]
enable-timestamp = true
disable-timestamp = true
`, nbUnset, nbUnset, nbTrue, nbUnset, false, true, IsNil)

	testLoad(`
[Log]
enable-error-stack = false
disable-error-stack = false
`, nbFalse, nbUnset, nbUnset, nbUnset, false, true, IsNil)

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
alter-primary-key = true
split-region-max-num=10000
enable-batch-dml = true
server-version = "test_version"
`)

	c.Assert(err, IsNil)
	c.Assert(f.Sync(), IsNil)

	c.Assert(conf.Load(configFile), IsNil)

	c.Assert(conf.ServerVersion, Equals, "test_version")
	c.Assert(mysql.ServerVersion, Equals, conf.ServerVersion)

	c.Assert(conf.AlterPrimaryKey, Equals, true)

	c.Assert(conf.TokenLimit, Equals, uint(1000))
	c.Assert(conf.SplitRegionMaxNum, Equals, uint64(10000))
	c.Assert(conf.EnableBatchDML, Equals, true)
	c.Assert(f.Close(), IsNil)
	c.Assert(os.Remove(configFile), IsNil)

	configFile = filepath.Join(filepath.Dir(localFile), "config.toml.example")
	c.Assert(conf.Load(configFile), IsNil)

	// Make sure the example config is the same as default config.
	c.Assert(conf, DeepEquals, GetGlobalConfig())

	// Test for log config.
	c.Assert(conf.Log.ToLogConfig(), DeepEquals, logutil.NewLogConfig("info", "text", conf.Log.File, false, func(config *zaplog.Config) { config.DisableErrorVerbose = conf.Log.getDisableErrorStack() }))
}
