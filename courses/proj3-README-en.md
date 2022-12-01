# DDL

## Introduction

DDL (Data Definition Language) is a language used to describe data table entities. Simply put, it is part of the logical implement of a database that creates/deletes/alters a library/table/column/index. This project will introduce TinySQL's DDL module in both the conceptual and code parts.

## Asynchronous Schema Changes

### Knowledge points

The asynchronous schema change in TinySQL follows the algorithm for schema changes in Google F1. You can learn [Asynchronous schema changes in F1](https://github.com/ngaut/builddatabase/blob/master/f1/schema-change.md) to basically understand the implement idea of asynchronous schema changes, and learn about the implement
process of TiDB through this article on [implement of asynchronous schema change in TiDB](https://github.com/ngaut/builddatabase/blob/master/f1/schema-change-implement.md). These will help you complete this course.

If you are interested in learning more about the derivation process of asynchronous schema changes, etc., you can refer to the original paper [Online, Asynchronous Schema Change in F1](http://static.googleusercontent.com/media/research.google.com/zh-CN//pubs/archive/41376.pdf). There is also optimization during TiDB implement
[TiDB asynchronous schema change optimization](http://zimulala.github.io/2017/12/24/optimize/).

### Related codes

The code in TinySQL is mainly in the package directory `ddl`. The following documents include the main features of the `ddl`

| File | Introduction |
| :------------- | :------------------------------------------ |
| `ddl.go` | Contains the DDL interface definition and its implement. |
| `ddl_api.go` | Provides an API for operations such as create, drop, alter, truncate, rename, etc., and can be called by the Executor. The main function is to encapsulate the DDL job, then store it in the DDL job queue, wait for the job to be executed and returned. |
| `ddl_worker.go` | DDL worker implement. The worker on the owner node takes the job from the job queue, executes it, and stores the job in the job history queue after execution is complete. |
| `syncer.go` | `schema version` responsible for synchronizing the ddl worker's owner and follower. `schema version ID` adds 1 every time the DDL state changes. |

The code related to `ddl owner` is placed separately in the `owner` directory, which implement functions such as owner elect.

Here, we mainly use `CreateTable` statements to introduce the implement logic in the code.

`create table` needs to parse the table's metadata ([tableInfo](https://github.com/pingcap-incubator/tinysql/blob/course/parser/model/model.go#L180)) from SQL, do some checks, and then store the table's metadata permanently in TiKV. In the DDL package, the exposed interface is [createTable](https://github.com/pingcap-incubator/tinysql/blob/course/ddl/ddl_api.go#L846). This function will be called in the execution layer. The main process is as follows:

* It will first check some restrictions, such as whether the table name already exists, whether the table name is too long, whether the column is defined repeatedly, etc.
* [buildTableInfo](https://github.com/pingcap-incubator/tinysql/blob/course/ddl/ddl_api.go#L712) takes the global table ID, generates `tableInfo`, that is, the table's metadata, and then encapsulates it into a DDL job. This job includes `table ID` and `tableInfo`, and marks the job's type as `ActionCreateTable`.
* The [D.addddlJob]() in the [D.doddljob]() function will first obtain a global job ID for the job and then put it in the job queue.
* After the DDL component starts, a `ddl_worker` coroutine is started in the [Start](https://github.com/pingcap-incubator/tinysql/blob/course/ddl/ddl.go#L285) function to run the [Start](https://github.com/pingcap-incubator/tinysql/blob/course/ddl/ddl_worker.go#L112) function. Every once in a while, the [handledDLJobQueue](https://github.com/pingcap-incubator/tinysql/blob/course/ddl/ddl_worker.go#L300) function is called to try to process the job in the DDL job queue. `ddl_worker` will first check if it is the owner, and if not, it will do nothing, and then return; if it is the owner, it will call the [getFirstDDLJob](https://github.com/pingcap-incubator/tinysql/blob/course/ddl/ddl_worker.go#L219) function to get the DDL function The first job in the queue, then the [RunddLjob](https://github.com/pingcap-incubator/tinysql/blob/course/ddl/ddl_worker.go#L427) function is called to execute the job.
* The [RunddLjob](https://github.com/pingcap-incubator/tinysql/blob/course/ddl/ddl_worker.go#L452) function will call the corresponding execution function according to the type of the job. For the `create table` type job, the [onCreateTable](https://github.com/pingcap-incubator/tinysql/blob/course/ddl/table.go#L32) function will be called, and after some checks, the [createTableOrViewWithCheck](https://github.com/pingcap-incubator/tinysql/blob/course/ddl/table.go#L66) function will be called, the function will be mapped to `db_ID` and `table_ID` as the value, and the configuration will be saved in TiKV as a value, and the job's will be updated `key` `tableInfo` status.
* The [finishddljob](https://github.com/pingcap-incubator/tinysql/blob/course/ddl/ddl_worker.go#L257) function removes the job from the DDL job queue and then joins the history ddl job queue.
* The [doddljob](https://github.com/pingcap-incubator/tinysql/blob/course/ddl/ddl.go#L449) function returns after detecting that there is a corresponding job in the history DDL job queue.

## Exercises Description

1. implement `updateVersionAndTableInfo` There will be an introduction to the method and some tips on the [updateVersionAndTableInfo](https://github.com/pingcap-incubator/tinysql/blob/course/ddl/table.go#L360) method. Follow the prompts to complete [codes](https://github.com/pingcap-incubator/tinysql/blob/course/ddl/table.go#L378).
2. Complete the implement of `Add Column`. There will be an introduction to the method and some tips on the [onAddcolumn](https://github.com/pingcap-incubator/tinysql/blob/course/ddl/column.go#L136) method. After understanding it, fill in the code in [The right location](https://github.com/pingcap-incubator/tinysql/blob/course/ddl/column.go#L194). The `onDropColumn` method used in this method will be completed as a second job.
3. Complete the implement of `Drop Column`. Also on the [onDropcolumn](https://github.com/pingcap-incubator/tinysql/blob/course/ddl/column.go#L216) method, there will be an introduction to the method and some tips. After understanding it, fill in the code in [The right location](https://github.com/pingcap-incubator/tinysql/blob/course/ddl/column.go#L245).
4. It is best for you to understand the entire process and idea of the above three questions as a whole, which may help you better understand the asynchronous schema change process.
5. Finally, the unit tests `TestAddColumn`, `TestDropColumn`, and `TestColumnChange` were passed.

## Tests

You can use `go test package_path -check.f func_name` to run a specific function.

You can also run all tests directly from the `tinysql` root directory using `make test-proj3`.

## Rating

Pass all tests on `make test-proj3` to get a perfect score.

### referencing

* [Online, Asynchronous Schema Change in F1](http://static.googleusercontent.com/media/research.google.com/zh-CN//pubs/archive/41376.pdf)
* [Asynchronous schema changes in F1](https://github.com/ngaut/builddatabase/blob/master/f1/schema-change.md)
* [TiDB Source Code Reading Series (17) DDL Source Code Analysis | PingCap](https://pingcap.com/zh/blog/tidb-source-code-reading-17)
* [implement of asynchronous schema change in TiDB](https://github.com/ngaut/builddatabase/blob/master/f1/schema-change-implement.md)
* [TiDB asynchronous schema change optimization](http://zimulala.github.io/2017/12/24/optimize/)
