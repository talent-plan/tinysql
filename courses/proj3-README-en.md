# Data Definition Language (DDL)

## Introduction

Data Definition Language (DDL) is used to create, modify, or delete a table or index in a database. This project will introduce TinySQL's DDL module in both the concepts and the implementations.

## Asynchronous Schema Changes

### Brief Introduction

The asynchronous schema change in TinySQL follows the algorithm for schema changes in Google F1. Please read the paper [Online, Asynchronous Schema Change in F1](http://static.googleusercontent.com/media/research.google.com/zh-CN//pubs/archive/41376.pdf). 

### Implementation

The TinySQL's DDL code is mainly in the package directory `ddl`. The following code implements the main features of the `ddl`

| File | Introduction |
| :------------- | :------------------------------------------ |
| `ddl.go` | Contains the DDL interface definition and its implementation. |
| `ddl_api.go` | Provides an API for operations, such as create, drop, alter, truncate, and rename. It can be called by the Executor. The main function is to encapsulate the DDL job, then store it in the DDL job queue, and wait for the job to be executed and returned. |
| `ddl_worker.go` | DDL worker implementation. The worker on the owner node takes the job from the job queue, executes it, and stores the job in the job history queue after execution is complete. |
| `syncer.go` | Responsible for synchronizing `schema version` between the ddl worker's owner and follower. `schema version ID` increases by 1 every time the DDL state changes. |

The code related to `ddl owner` is placed separately in the `owner` directory, which implement functions such as owner election.

Here, we mainly use `CreateTable` statements to introduce the implement logic in the code.

`create table` parse the table's metadata ([tableInfo](https://github.com/pingcap-incubator/tinysql/blob/course/parser/model/model.go#L180)) from SQL, perform some checks, and then store the table's metadata in TiKV. In the DDL package, the exposed interface is [createTable](https://github.com/pingcap-incubator/tinysql/blob/course/ddl/ddl_api.go#L846). This function will be called in the execution layer. The main process is as follows:

* It will first check some constrains, such as whether the table name already exists, whether the table name is too long, and whether the column is defined repeatedly.
* [buildTableInfo](https://github.com/pingcap-incubator/tinysql/blob/course/ddl/ddl_api.go#L712) takes the global table ID, generates `tableInfo`, the table's metadata, and then encapsulates it into a DDL job. This job includes `table ID` and `tableInfo`, and marks the job's type as `ActionCreateTable`.
* The [d.addddlJob]() in the [d.doddljob]() function will first obtain a global job ID for the job and then put it in the job queue.
* After the DDL component starts, a `ddl_worker` coroutine is started in the [Start](https://github.com/pingcap-incubator/tinysql/blob/course/ddl/ddl.go#L285) function. Every once in a while, the [handledDLJobQueue](https://github.com/pingcap-incubator/tinysql/blob/course/ddl/ddl_worker.go#L300) function is called to try to process the job in the DDL job queue. `ddl_worker` will first check if it is the owner. If not, it will do nothing; if it is the owner, it will call the [getFirstDDLJob](https://github.com/pingcap-incubator/tinysql/blob/course/ddl/ddl_worker.go#L219) function to get the first job in the queue, and then call the [runddLjob](https://github.com/pingcap-incubator/tinysql/blob/course/ddl/ddl_worker.go#L427) function to execute the job.
* The [runddLjob](https://github.com/pingcap-incubator/tinysql/blob/course/ddl/ddl_worker.go#L452) function will call the corresponding execution function according to the type of the job. For the `create table` type job, the [onCreateTable](https://github.com/pingcap-incubator/tinysql/blob/course/ddl/table.go#L32) function will be called. Then, the [createTableOrViewWithCheck](https://github.com/pingcap-incubator/tinysql/blob/course/ddl/table.go#L66) function will also be called, where the `db_ID` and `table_ID` will be mapped as the key, and the configuration will be saved in TiKV as a value, and the job's will be updated `key` `tableInfo` status.
* The [finishddljob](https://github.com/pingcap-incubator/tinysql/blob/course/ddl/ddl_worker.go#L257) function removes the job from the DDL job queue and then joins the history ddl job queue.
* The [doddljob](https://github.com/pingcap-incubator/tinysql/blob/course/ddl/ddl.go#L449) function returns after detecting that there is a corresponding job in the history DDL job queue.

## Task

1. Implement `updateVersionAndTableInfo`. There will be an introduction to the method and some tips on the [updateVersionAndTableInfo](https://github.com/pingcap-incubator/tinysql/blob/course/ddl/table.go#L360) method. Follow the prompts to complete [codes](https://github.com/pingcap-incubator/tinysql/blob/course/ddl/table.go#L378).
2. Complete the implementation of `Add Column`. There will be an introduction to the method and some tips on the [onAddcolumn](https://github.com/pingcap-incubator/tinysql/blob/course/ddl/column.go#L136) method. After reading it, fill in the code in [The right location](https://github.com/pingcap-incubator/tinysql/blob/course/ddl/column.go#L194). The `onDropColumn` method used in this method will be completed as a second job.
3. Complete the implementation of `Drop Column`. Also on the [onDropcolumn](https://github.com/pingcap-incubator/tinysql/blob/course/ddl/column.go#L216) method, there will be an introduction to the method and some tips. After reading it, fill in the code in [the right location](https://github.com/pingcap-incubator/tinysql/blob/course/ddl/column.go#L245).
4. Finally, check you code by the unit tests `TestAddColumn`, `TestDropColumn`, and `TestColumnChange`.

## Tests

You can use `go test package_path -check.f func_name` to run a specific function.

You can also run all tests directly from the `tinysql` root directory using `make test-proj3`.

## Grading

Pass all tests on `make test-proj3`.
