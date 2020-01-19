module github.com/pingcap/tidb

require (
	github.com/BurntSushi/toml v0.3.1
	github.com/blacktear23/go-proxyprotocol v0.0.0-20180807104634-af7a81e8dd0d
	github.com/coreos/go-systemd v0.0.0-20181031085051-9002847aa142 // indirect
	github.com/coreos/pkg v0.0.0-20180928190104-399ea9e2e55f // indirect
	github.com/cznic/mathutil v0.0.0-20181122101859-297441e03548
	github.com/cznic/parser v0.0.0-20181122101858-d773202d5b1f
	github.com/cznic/sortutil v0.0.0-20181122101858-f5f958428db8
	github.com/cznic/strutil v0.0.0-20181122101858-275e90344537
	github.com/cznic/y v0.0.0-20181122101901-b05e8c2e8d7b
	github.com/dustin/go-humanize v1.0.0 // indirect
	github.com/go-sql-driver/mysql v0.0.0-20170715192408-3955978caca4
	github.com/gogo/protobuf v1.2.1
	github.com/golang/groupcache v0.0.0-20190702054246-869f871628b6 // indirect
	github.com/golang/protobuf v1.3.2
	github.com/google/btree v1.0.0
	github.com/google/uuid v1.1.1
	github.com/gorilla/context v1.1.1 // indirect
	github.com/gorilla/mux v1.6.2
	github.com/gorilla/websocket v1.4.0 // indirect
	github.com/konsorten/go-windows-terminal-sequences v1.0.2 // indirect
	github.com/montanaflynn/stats v0.0.0-20180911141734-db72e6cae808 // indirect
	github.com/ngaut/pools v0.0.0-20180318154953-b7bc8c42aac7
	github.com/ngaut/sync2 v0.0.0-20141008032647-7a24ed77b2ef // indirect
	github.com/opentracing/opentracing-go v1.0.2
	github.com/pingcap/check v0.0.0-20191107115940-caf2b9e6ccf4
	github.com/pingcap/errors v0.11.5-0.20190809092503-95897b64e011
	github.com/pingcap/failpoint v0.0.0-20191029060244-12f4ac2fd11d
	github.com/pingcap/goleveldb v0.0.0-20171020122428-b9ff6c35079e
	github.com/pingcap/kvproto v0.0.0-20191211054548-3c6b38ea5107
	github.com/pingcap/log v0.0.0-20191012051959-b742a5d432e9
	github.com/pingcap/parser v0.0.0-20200118095137-622be79f68c1
	github.com/pingcap/pd v1.1.0-beta.0.20191210055626-676ddd3fbd2d
	github.com/pingcap/tipb v0.0.0-20191209145133-44f75c9bef33
	github.com/sirupsen/logrus v1.2.0
	github.com/soheilhy/cmux v0.1.4
	github.com/spaolacci/murmur3 v0.0.0-20180118202830-f09979ecbc72
	github.com/unrolled/render v0.0.0-20180914162206-b9786414de4d // indirect
	go.etcd.io/etcd v0.5.0-alpha.5.0.20191023171146-3cf2f69b5738
	go.uber.org/atomic v1.5.0
	go.uber.org/automaxprocs v1.2.0
	go.uber.org/zap v1.12.0
	golang.org/x/crypto v0.0.0-20191206172530-e9b2fee46413 // indirect
	golang.org/x/net v0.0.0-20190909003024-a7b16738d86b
	golang.org/x/sys v0.0.0-20191210023423-ac6580df4449 // indirect
	golang.org/x/text v0.3.2
	golang.org/x/time v0.0.0-20190308202827-9d24e82272b4 // indirect
	golang.org/x/tools v0.0.0-20191107010934-f79515f33823
	google.golang.org/genproto v0.0.0-20190905072037-92dd089d5514 // indirect
	google.golang.org/grpc v1.25.1
	gopkg.in/check.v1 v1.0.0-20190902080502-41f04d3bba15 // indirect
	gopkg.in/natefinch/lumberjack.v2 v2.0.0
)

go 1.13

replace github.com/pingcap/check => github.com/tiancaiamao/check v0.0.0-20191119042138-8e73d07b629d
