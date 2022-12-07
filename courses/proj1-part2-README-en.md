# Section II Table Codec

## Overview

In this section, we will explain how the data in the table is mapped to the storage engine, TinyKV.

## Introduction to TableCodec

Based on the data processing patterns mentioned in the previous section, here are a few requirements for data storage:
- Data from the same table should be stored together. So that less storage blocks need to be retrieved when reading and/or writing data within the one table. IO may be a huge bottleneck at least for some workloads under certain hardware settings.
- The data arrangement within one table also depends on the query patterns:
  - If the point query dominates, the hash table may be a better design. Yet, a sorted array may also work.
  - If most of the queries are range queries, an sorted array might be better.
- Whether to use row-store or column-store is also determined by query patterns. If the data from the same row needs to be read and/or written together quite often, it is better to be stored together.

In TinyKV, we use a sorted array to arrange data. Both the key and the value will be serialized to bytes before storing into the store engine.

TinyKV should provide the following APIs:
- scan(startKey): Given a startKey, return all the values in order whose keys are greater than or equal to startKey.
- set(key, value): Update the value of a key.

From the discussion above, we can conclude that we need to store the data in the following way:
- A unique table identifier should be places at the beginning of the key, since all the data from the same table needs to be stored together;
- If a column needs to be sorted, then the value of this column needs to be placed inside key and right after the table unique identifier;
- Value includes all the rest data.

Concretely, every table will have a `TableID`, and each row will have a `RowID`. In the case where the primary key is an integer, the primary key will be the `RowID`. `TableID` is unique in the whole cluster, and `RowID` is unique in the table. Both `TableID` and `RowID` are all `int64` types. There are also `tablePrefix` and `RecordPrefixSep` inside the key. There are both string constants for differentiating with other data within the Key-Value space. 
```
    Key： tablePrefix_tableID_recordPrefixSep_rowID
    Value: [col1, col2, col3, col4]
```

In terms of indexes, each index is assigned a unique `indexID` within the table. The `indexID` will also be encoded into the key-value pair:

```
    Key: tablePrefix_tableID_indexPrefixSep_indexID_indexColumnsValue
    Value: rowID
```

For non-unique indexes, we also encode `rowID` into the key:

```
   Key: tablePrefix_tableID_indexPrefixSep_indexID_ColumnsValue_rowID
   Value：null
```

You may also consider how the data should be stored if `join` operation dominates the workload.

## Understanding the Code

The main code for `tablecodec` is in [tablecodec.go](https://github.com/pingcap-incubator/tinysql/blob/course/tablecodec/tablecodec.go) line 33 to 47.

In line 33 to 37, three constants mentioned above are defined: `tablePrefix`, `recordPrefixSep`, and `indexPrefixSep`.

In line 64, [encoderowkeyWithHandle](https://github.com/pingcap-incubator/tinysql/blob/course/tablecodec/tablecodec.go#L64) implements the encoding of row data.

In line 86, [encodeIndexSeekKey](https://github.com/pingcap-incubator/tinysql/blob/course/tablecodec/tablecodec.go#L86) implements the encoding of index data.

## Task

Implement [decodeRecordKey](https://github.com/pingcap-incubator/tinysql/blob/course/tablecodec/tablecodec.go#L72) and [decodeIndexKeyprefix](https://github.com/pingcap-incubator/tinysql/blob/course/tablecodec/tablecodec.go#L95) according to [encoderowkeyWithHandle](https://github.com/pingcap-incubator/tinysql/blob/course/tablecodec/tablecodec.go#L64) and [encodeIndexSeekKey](https://github.com/pingcap-incubator/tinysql/blob/course/tablecodec/tablecodec.go#L86) above. Note that since the parameter `key` may be illegal, error handling needs to be considered.

## Tests

Pass all tests under [Tablecodec](https://github.com/pingcap-incubator/tinysql/blob/course/tablecodec).

You can run all tests by `make test-proj1` in the root directory.

You can use `go test <package_path> -check.f <func_name>` to run a specific function. 

Using `TestDecodeIndexKey` as an example, you can use `go test github.com/pingcap/tidb/tablecodec -check.f TestDecodeIndexKey` to run this specific function.

## Grading

- `decodeRecordKey` 50%
- `decodeIndexKeyprefix` 50%

