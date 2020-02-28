# TinySQL

TinySQL is a course designed to teach you how to implement a distributed relational database in Go. TinySQL is also the name of the simplifed version of [TiDB](https://github.com/pingcap/tidb).

> **Note:**
>
> This course is still working in progress.

## Prerequisites

Experience with Go is required. If not, it is recommended to learn [A Tour of Go](https://tour.golang.org/) first.

## Course Overview

This course will take you from idea to implementation, with the essential topics of distributed relational database covered. 

The course is organized into three parts:

1. Gives a simple interpretation of SQL and relational algebra in preparation for the following course.

2. Explains the life of a read-only SQL, which includes parsing, execution, and the optimization of SQL plans.

3. Focuses on SQLs (including DML and DDL) that change the state of the database: how they get implemented and how to deal with the interaction of them and read-only statements.

## Other courses in this series

This course only focuses on the SQL layer of a distributed database system. If you are also interested in KV layer, see [TinyKV](https://github.com/pingcap-incubator/tinykv).

## Contributing

Contributions are welcomed and greatly appreciated. See [CONTRIBUTING.md](https://github.com/pingcap/community/blob/master/CONTRIBUTING.md) for details on submitting patches and the contribution workflow.

## License

TinySQL is under the Apache 2.0 license. See the [LICENSE](./LICENSE) file for details.
