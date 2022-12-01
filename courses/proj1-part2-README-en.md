# Table Codec

## Overview

In this chapter, we'll explain how the data on the table is mapped to TinyKV.

## Introduction to Table Codec

Judging from the data processing described in the previous section, what kind of properties do we need to store:

- First of all, from the perspective of single table operation, data from the same table should be stored together, so we can avoid reading data from other tables when processing one table
- Second, how should we arrange the data in the same table? From the perspective of SQL filter conditions
- If there are basically no filters, then it doesn't matter how you arrange them
- If there are many equal value queries such as person = "xx", then an arrangement like a hash table is better, of course, an ordered array is fine
- If there are many queries like number >= "xx", then an arrangement like an ordered array is better, because in this way we can avoid reading more useless data
- Should we store the data on the same line separately or together? Once again, it depends on the data access pattern. If data on the same row always needs to be read at the same time, then it is better to exist together. In TinySQL, we chose to store data on the same row together.

From the above perspective, an arrangement similar to an sorted array is probably the easiest way, since it can almost satisfy all requirements in a uniform manner. Next, let's take a look at TinyKV. From the simplest point of view, we can think of it as a KV engine that provides the following properties:

- Both Key and Value are bytes arrays, which means that regardless of the original type, we need to serialize them before storing them
- Scan (startKey), given any Key, this interface can return all data greater than or equal to this startKey in order.
- Set (key, value) to set the value of key to value.

Combined with the above discussion, the data storage method is ready to come up:

- Since the same table needs to be stored together, the unique label of the table should be placed in front of the Key, so the keys of the same table are continuous
- For a table, place the columns that need to be sorted after the table's unique label and coded in the Key
- Value holds all the other columns on a row

Specifically, we will assign each table a tableID, and each row will be assigned a rowID (if the table has int Primary Key, then the value of Primary Key will be used as rowID), where tableId is unique within the whole cluster and rowId is unique within the table. These IDs are all int64 types.
Each row of data is encoded into a key-value pair according to the following rules:

```
    Key： tablePrefix_tableID_recordPrefixSep_rowID
    Value: [col1, col2, col3, col4]
```

Among them, Key's tablePrefix/RecordPrefixSep are all specific string constants used to differentiate other data within the KV space.
For indexes, each index is assigned a unique IndexID within the table, and then encoded into a key-value pair according to the following rules:

```
    Key: tablePrefix_tableID_indexPrefixSep_indexID_indexColumnsValue
    Value: rowID
```

Of course, we also need to consider non-unique indexes. At this point, the above method won't work. We need to also encode RowID into the Key to make it unique:

```
   Key: tablePrefix_tableID_indexPrefixSep_indexID_ColumnsValue_rowID
   Value：null
```

Thoughts: If you think about it from a join perspective, how should the data be mapped?

## Understanding the Code

The main code for tablecodec is in [tablecodec.go](https://github.com/pingcap-incubator/tinysql/blob/course/tablecodec/tablecodec.go), and the code we need to focus on this time is mainly between [L33 to L147](https://github.com/pingcap-incubator/tinysql/blob/course/tablecodec/tablecodec.go#L33-L146).

At the beginning of the code, the three constants mentioned above are defined: tablePrefix, RecordPrefixSep, and indexPrefixSep.

Next, you can see that [encoderowkeyWithHandle](https://github.com/pingcap-incubator/tinysql/blob/course/tablecodec/tablecodec.go#L64) and [encodeIndexSeekKey](https://github.com/pingcap-incubator/tinysql/blob/course/tablecodec/tablecodec.go#L86) respectively implement the encoding of row data and index data as described above.

## Job Description

Implement [DecodeRecord Key](https://github.com/pingcap-incubator/tinysql/blob/course/tablecodec/tablecodec.go#L72) and [decodeIndexKeyprefix](https://github.com/pingcap-incubator/tinysql/blob/course/tablecodec/tablecodec.go#L95) according to [encoderowkeyWithHandle](https://github.com/pingcap-incubator/tinysql/blob/course/tablecodec/tablecodec.go#L64) and [encodeIndexSeekKey](https://github.com/pingcap-incubator/tinysql/blob/course/tablecodec/tablecodec.go#L86) above. Note that since the parameter `key` may be illegal, error handling needs to be considered.

## Tests

Passed all tests under [Tablecodec](https://github.com/pingcap-incubator/tinysql/blob/course/tablecodec).

You can run all tests via `make test-proj1` in the root directory.

You can use `go test package_path -check.f func_name` to run a specific function. Take `TestDecodeIndexKey` as an example,
You can use `go test github.com/pingcap/tidb/tablecodec -check.f TestDecodeIndexKey` to run this specific function.

## Rating

`TestDecodeIndexKey` and `TestRecordKey` each scored 50 percent.

