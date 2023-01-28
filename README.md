# TinySQL

TinySQL is a course designed to teach you how to implement a distributed relational database in Go. The name TinySQL indicates it is a simplifed version of [TiDB](https://github.com/pingcap/tidb).

## Prerequisites

Experience with Go is required. If not, it is recommended to learn [A Tour of Go](https://tour.golang.org/) first.

## Course Architecture

The entire course is designed based on [TiDB](https://github.com/pingcap/tidb).

The course is organized into six parts:

- Simple explanation of SQL and relational algebra[English](./courses/proj1-README-en.md)|[Chinese](./courses/proj1-README-zh_CN.md)
- [Parser](./courses/proj2-README-zh_CN.md)
- [DDL](./courses/proj3-README-zh_CN.md)
- [Optimizer](./courses/proj4-README-zh_CN.md)
- [Executor](./courses/proj5-README-zh_CN.md)
- [Percolator](./courses/proj6-README-zh_CN.md)

## Reference List

This [reference list](./courses/material.md) provides plenty of materials for you to study how a database system works. We pick some topics in it and prepare homework for you to have a better understand about it.

This course will take you from idea to implementation, with the essential topics of distributed relational database covered. 

The course is organized into three parts:

1. Gives a simple interpretation of SQL and relational algebra in preparation for the following course.

2. Explains the life of a read-only SQL, which includes parsing, execution, and the optimization of SQL plans.

3. Focuses on SQLs (including DML and DDL) that change the state of the database: how they get implemented and how to deal with the interaction of them and read-only statements.

## Other courses in this series

This course only focuses on the SQL layer of a distributed database system. If you are also interested in KV layer, see [TinyKV](https://github.com/pingcap-incubator/tinykv).

## Deploy

Once you finish the project. You can deploy the binary and use MySQL client to connect the server.

### Build

```
make
```

### Run & Play

Use `./bin/tidb-server` to deploy the server and use `mysql -h127.0.0.1 -P4000 -uroot` to connect the server.

Also you can deploy the cluster together with the tinykv project.

Use `make` command in tinykv project to build binary `tinyscheduler-server`, `tinykv-server`.

Then put the binaries into a single dir and run the following commands:

```
mkdir -p data
./tinyscheduler-server
./tinykv-server -path=data
./tidb-server --store=tikv --path="127.0.0.1:2379"
```
## Start to learn and join the learning class

Please register the talentplan course platform and join the [tinySQL learning class](https://talentplan.edu.pingcap.com/catalog/info/id:234). By the way, you can find some other fundamental courses in the learning platform.

## Autograding

Since Jun 2022, we start using [github classroom](./classroom.md) to accept labs and provide autograding timely. The github classroom invitation, the video content and the certification after you pass the class is  provided in the [tinySQL learning class](https://talentplan.edu.pingcap.com/catalog/info/id:234)

## Contributing

Contributions are welcomed and greatly appreciated. See [Contribution Guide](https://github.com/pingcap/community/tree/master/contributors) for details on submitting patches and the contribution workflow.

## License

TinySQL is under the Apache 2.0 license. See the [LICENSE](./LICENSE) file for details.
