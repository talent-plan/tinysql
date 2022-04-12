# Table Codec

## 概览

在本章我们将介绍表上的数据如何映射到 TinyKV 上。

## Table Codec 简介

从上一节介绍的数据处理看，我们需要存储具有怎样的性质呢：

- 首先，从单表操作的角度看，同一张表的数据应当是存放在一起的，这样我们可以避免在处理某一张表时读取到其他表上的数据
- 其次，对于同一张表内的数据，我们该如何排列呢？从 SQL 过滤条件的角度来看
	- 如果基本上没有过滤条件，那么无论怎么排列都没关系
	- 如果类似 person = “xx” 这样的等值查询比较多，那么一个类似 hash 表的排列是比较优的，当然一个有序数组也可以
	- 如果类似 number >= “xx” 这样的查询比较多，那么一个类似有序数组的排列是比较优的，因为这样我们可以避免读取较多的无用数据
- 对于同一行上的数据，我们是分开存还是存在一起比较好呢？再一次，这取决于数据访问的模式，如果同一行上的数据总是需要同时被读取，那么存在一起是更好的，在 TinySQL 中我们选择了将同一行上的数据存放在一起。

从上面的角度看，一个类似有序的数组的排列可能是最简单的方式，因为几乎它的可以用一个统一的方式满足所有的要求。接下来我们再看看 TinyKV，从最简单的角度看，我们可以将它看做一个提供了如下性质的 KV 引擎：

- Key 和 Value 都是 bytes 数组，也就是说无论原先的类型是什么，我们都要序列化后再存入
- Scan(startKey)，任意给定一个 Key，这个接口可以按顺序返回所有大于等于这个 startKey 数据。
- Set(key, value)，将 key 的值设置为 value。

结合上面的讨论，数据的存储方式就呼之欲出了：

- 由于同一张表的需要存放在一起，那么表的唯一标示应该放在 Key 的最前面，这样同一张表的 Key 就是连续的
- 对于某一张表，将需要排序的列放在表的唯一标示后面，编码在 Key 里
- Value 中存放某一行上所有其他的列

具体来说，我们会对每个表分配一个 TableID，每一行分配一个 RowID（如果表有整数型的 Primary Key，那么会用 Primary Key 的值当做 RowID），其中 TableID 在整个集群内唯一，RowID 在表内唯一，这些 ID 都是 int64 类型。
每行数据按照如下规则进行编码成 Key-Value pair：

```
    Key： tablePrefix_tableID_recordPrefixSep_rowID
    Value: [col1, col2, col3, col4]
```

其中 Key 的 tablePrefix/recordPrefixSep 都是特定的字符串常量，用于在 KV 空间内区分其他数据。
对于索引，会为每一个索引分配表内唯一的 indexID，然后按照如下规则编码成 Key-Value pair：

```
    Key: tablePrefix_tableID_indexPrefixSep_indexID_indexColumnsValue
    Value: rowID
```

当然，我们还需要考虑非唯一索引，这个时候上面的方法就行不通了，我们需要将 rowID 也编码进 Key 里使之成为唯一的：

```
   Key: tablePrefix_tableID_indexPrefixSep_indexID_ColumnsValue_rowID
   Value：null
```

思考：如果从 join 的角度考虑，数据应该怎么映射呢？

## 理解代码

tablecodec 的主要代码位于 [tablecodec.go](https://github.com/pingcap-incubator/tinysql/blob/course/tablecodec/tablecodec.go)，这次我们需要关注的代码主要从 [L33 到 L147](https://github.com/pingcap-incubator/tinysql/blob/course/tablecodec/tablecodec.go#L33-L146) 之间。

代码一开始，定义了上文提到的三个常量：tablePrefix，recordPrefixSep 和 indexPrefixSep。

接下来可以看到 [EncodeRowKeyWithHandle](https://github.com/pingcap-incubator/tinysql/blob/course/tablecodec/tablecodec.go#L64) 和 [EncodeIndexSeekKey](https://github.com/pingcap-incubator/tinysql/blob/course/tablecodec/tablecodec.go#L86) 分别实现了上述所说的行数据和索引数据的编码。

## 作业描述

根据上述 [EncodeRowKeyWithHandle](https://github.com/pingcap-incubator/tinysql/blob/course/tablecodec/tablecodec.go#L64) 和 [EncodeIndexSeekKey](https://github.com/pingcap-incubator/tinysql/blob/course/tablecodec/tablecodec.go#L86)，实现 [DecodeRecordKey](https://github.com/pingcap-incubator/tinysql/blob/course/tablecodec/tablecodec.go#L72) 和 [DecodeIndexKeyPrefix](https://github.com/pingcap-incubator/tinysql/blob/course/tablecodec/tablecodec.go#L95)，注意由于参数 `key` 可能是不合法的，需要考虑错误处理。

## 测试

通过 [tablecodec](https://github.com/pingcap-incubator/tinysql/blob/course/tablecodec) 下所有测试。

你可以在根目录通过 `make test-proj1` 来执行所有测试。

你可以通过 `go test package_path -check.f func_name` 来跑一个具体的函数。以 `TestDecodeIndexKey` 为例，
你可以使用 `go test github.com/pingcap/tidb/tablecodec -check.f TestDecodeIndexKey` 来跑这个具体的函数。

## 评分

`TestDecodeIndexKey`  和 `TestRecordKey` 各占 50% 分数。

