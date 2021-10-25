# The TinySQL Course

The TinySQL course builds a learning-friendly mini distributed relational database. It is inspired by [TiDB Project](https://github.com/pingcap/tidb).

In this course, you will build a distributed relational database by yourself. You will also learn about the design and implementation of distributed database.

## Course Architecture

The whole project is a skeleton code for a server compatible whith the MySQL protocol. You need to finish the core code step by step:

- Stage 1: read-only relational database
  - Target: the ability to read data using KV engine API
  - Iconic function: the ability to handle simple SELECT statements
  - Knowledge topic:
        - parser
        - data mapping from the relational model to KV
        - generating operator
- Stage 2: insert and update
  - Target: the ability to write data using KV engine API
  - Iconic function: the ability to handle simple INSERT/UPDATE statements
  - Knowledge topic:
    - volcano model
- Stage 3: DDL
  - Target: the ability to process DDL online
  - Iconic function: the ability to process CREATE/DROP TABLE/INDEX online
  - Knowledge topic:
    - online DDL algorithm
- Stage 4: Optimizer
  - Target: implement an optimizer and be able to choose the appropriate index and Join Order
  - Iconic function:
    - ability to collect statistics
    - ability to choose the appropriate index and Join Order
  - Knowledge topic:
    - SQL optimization
    - statistics
    - SystemR optimizer
- Stage 5: Calculation optimization
  - Target: optimize the calculation framework to improve performance
  - Iconic function:
    - vectorization
    - Massively Parallel Processing(MPP)
  - Knowledge topic:
    - vectorization
    - MPP

## Code Structure

TODO

## Deploy

### Build

```
make
```

### Run & Play

Use `./bin/tinysql-server` to run the server and use MySQL client `mysql --host 127.0.0.1 --port 4000 -u root` to connect the server.

## Contributing

Any feedback and contribution is greatly appreciated. Please see [issues](https://github.com/tidb-incubator/tinysql/issues) if you want to join in the development.

## License

TinySQL is under the Apache 2.0 license.
