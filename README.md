# TinySQL

A working in progress course about the implementation of distributed relational database in Go.
TinySQL is a simplified version of [TiDB](https://github.com/pingcap/tidb).

## The goal of this course

This course tries to teach the essential parts of distributed relational database in a simple to complex and
static to dynamic way:
- First, it will give a simple interpretation of SQL and relational algebra in preparation for the following course.
- Next, it will explain the life of a read-only SQL, which includes parsing, execution, and the optimization of SQL plans.
- Finally, it will focus on SQLs (including DML and DDL) that change the state of the database: how they get implemented
  and how to deal with the interaction of them and read-only statements.

## Prerequisites

Go language experience is required. If not, it is recommended to learn [A Tour of Go](https://tour.golang.org/) first.

## Other courses in this series

This course only focuses on the SQL layer of a distributed database system. For those who are also interested in KV layer may
turn to [TinyKV](https://github.com/pingcap-incubator/tinykv).  

## Contributing
Contributions are welcomed and greatly appreciated. See
[CONTRIBUTING.md](https://github.com/pingcap/community/blob/master/CONTRIBUTING.md)
for details on submitting patches and the contribution workflow.

## License
TinySQL is under the Apache 2.0 license. See the [LICENSE](./LICENSE) file for details.
