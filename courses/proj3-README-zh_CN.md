# DDL

## 概述

DDL(Data Definition Language) 数据模式定义语言，是用来描述数据表实体的语言。简单来说就是数据库中对库/表/列/索引进行创建/删除/变更操作的部分逻辑实现。这个 Project 中会对 TinySQL 的 DDL 模块在概念和代码两部分上进行一些介绍。

## 异步 schema 变更

TinySQL 中的异步 schema 变更是参照了 Google F1 中的 schema 变更的算法。F1 的原始论文可以在[http://static.googleusercontent.com/media/research.google.com/zh-CN//pubs/archive/41376.pdf] 中找到。同时在[这里](https://github.com/ngaut/builddatabase/blob/master/f1/schema-change.md)有一份中文版的说明。

## 理解代码

TinySQL 中的代码主要在包目录 `ddl` 中。其中下述文件包含了 `ddl` 的主要功能

| File | Introduction |
| :------------- | :------------------------------------------ | 
| `ddl.go` | 包含 DDL 接口定义和其实现。 |
| `ddl_api.go` | 提供 create , drop , alter , truncate , rename 等操作的 API，供 Executor 调用。主要功能是封装 DDL 操作的 job 然后存入 DDL job queue，等待 job 执行完成后返回。| 
| `ddl_worker.go` | DDL worker 的实现。owner 节点的 worker 从 job queue 中取 job，然后执行，执行完成后将 job 存入 job history queue 中。| 
| `syncer.go` | 负责同步 ddl worker 的 owner 和 follower 间的 `schema version`。 每次 DDL 状态变更后 `schema version ID` 都会加 1。|

`ddl owner` 相关的代码单独放在 `owner` 目录下，实现了 owner 选举等功能。

这里我们主要以 `CreateTable` 语句来介绍一下代码中的实现逻辑。

`create table` 需要把 table 的元信息（[TableInfo](https://github.com/pingcap-incubator/tinysql/blob/course/parser/model/model.go#L180)）从 SQL 中解析出来，做一些检查，然后把 table 的元信息持久化保存到 TiKV 中。在 DDL 包中，对外暴露的接口是 [CreateTable](https://github.com/pingcap-incubator/tinysql/blob/course/ddl/ddl_api.go#L846)。这个函数会在执行层中调用。其主要流程如下：

* 会先 check 一些限制，比如 table name 是否已经存在，table 名是否太长，是否有重复定义的列等等限制。
* [buildTableInfo](https://github.com/pingcap-incubator/tinysql/blob/course/ddl/ddl_api.go#L712) 获取 global table ID，生成 `tableInfo` , 即 table 的元信息，然后封装成一个 DDL job，这个 job 包含了 `table ID` 和 `tableInfo`，并将这个 job 的 type 标记为 `ActionCreateTable`。
* [d.doDDLJob(ctx, job)](https://github.com/pingcap-incubator/tinysql/blob/course/ddl/ddl.go#L421) 函数中的 [d.addDDLJob(ctx, job)](https://github.com/pingcap-incubator/tinysql/blob/course/ddl/ddl.go#L182) 会先给 job 获取一个 global job ID 然后放到 job queue 中去。
* DDL 组件启动后，在 [start](https://github.com/pingcap-incubator/tinysql/blob/course/ddl/ddl.go#L285) 函数中会启动一个 `ddl_worker` 协程运行 [start](https://github.com/pingcap-incubator/tinysql/blob/course/ddl/ddl_worker.go#L112) 函数，每隔一段时间调用 [handleDDLJobQueue](https://github.com/pingcap-incubator/tinysql/blob/course/ddl/ddl_worker.go#L300) 函数去尝试处理 DDL job 队列里的 job，`ddl_worker` 会先 check 自己是不是 owner，如果不是 owner，就什么也不做，然后返回；如果是 owner，就调用 [getFirstDDLJob](https://github.com/pingcap-incubator/tinysql/blob/course/ddl/ddl_worker.go#L219) 函数获取 DDL 队列中的第一个 job，然后调 [runDDLJob](https://github.com/pingcap-incubator/tinysql/blob/course/ddl/ddl_worker.go#L427) 函数执行 job。
	* [runDDLJob](https://github.com/pingcap-incubator/tinysql/blob/course/ddl/ddl_worker.go#L452) 函数里面会根据 job 的类型，然后调用对应的执行函数，对于 `create table` 类型的 job，会调用 [onCreateTable](https://github.com/pingcap-incubator/tinysql/blob/course/ddl/table.go#L32) 函数，然后做一些 check 后，会调用 [createTableOrViewWithCheck](https://github.com/pingcap-incubator/tinysql/blob/course/ddl/table.go#L66) 函数，将 `db_ID` 和 `table_ID` 映射为 `key`，`tableInfo` 作为 value 存到 TiKV 里面去，并更新 job 的状态。
* [finishDDLJob](https://github.com/pingcap-incubator/tinysql/blob/course/ddl/ddl_worker.go#L257) 函数将 job 从 DDL job 队列中移除，然后加入 history ddl job 队列中去。
* [doDDLJob](https://github.com/pingcap-incubator/tinysql/blob/course/ddl/ddl.go#L449) 函数中检测到 history DDL job 队列中有对应的 job 后，返回。

## 作业

我们这里实现一下比较简单的 `Drop Column` 涉及的[一些代码](https://github.com/pingcap-incubator/tinysql/blob/course/ddl/column.go#L216)。

## 测试

通过单元测试 `TestColumnChange` 以及 `TestDropColumn`。
