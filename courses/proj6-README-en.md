# Distributed Transaction: Percolator

## Overview

In this section, we'll implement the Percolator submission agreement.

## Percolator

[Percolator](https://research.google/pubs/pub36726/) is a distributed transaction processing protocol disclosed by Google in 2010. The Percolator protocol divides transaction submission into two stages: Prewrite and Commit, enabling a distributed transaction database without a coordinator. Google Spanner uses this protocol to implement a globally deployed distributed database with GPS and an atomic clock synchronization protocol. Because of its excellent scalability, it is also used by various distributed database, and the transaction implement of TiDB and TinySQL is also based on this agreement.

## Introduction to Execution Models

In TiDB, the execution process of the transaction is cached in a buffer, and only when it is submitted is it completely written to the distributed TiKV storage engine through the Percolator submission protocol. The entry point for this call is the `tikvTxn.Commit` function in `store/tikv/txn.go`.

At the time of execution, one transaction may encounter other transactions during execution. At this time, it is necessary to use the Lock Resolve component to query the status of the encountered transaction and execute the corresponding action based on the results of the query.

## Two Phase Commit

The two-stage submission of the Percolator submission agreement is divided into Prewrite and Commit: Prewrite actually writes the data, and Commit makes the data visible. Among them, the success of a transaction is atomically marked by the Primary Key. When Prewrite fails or Primary Key Commit fails, garbage cleaning is required, and written transaction are rolled back.

The key in a transaction may involve different regions. When the key is written, it needs to be sent to the correct region before it can be processed. The `GroupKeysByRegion` function divides the key into multiple batches by region according to the region cache, but there may be cases where the corresponding storage node returns a Region Error due to the cache expiration. In this case, you need to split the batch and try again.

### TODO

In order to be able to perform operations on the Key, it is necessary to implement the `GroupKeysByRegion` function in `region_cache.go`.

In the execution process, three types of operations are involved, namely Prewrite/Commit/Rollback (Cleanup). These actions are handled in the same process. You need to complete the `buildPrewriteRequest` function during the Prewrite process, then follow the `handleSingleBatch` function of Prewrite and complete the `handleSingleBatch` function of the Commit and Rollback.

## Lock Resolver

In the prewrite phase, two records are written for operations on one key.

- The actual KV data is stored in Default CF.
- Lock CF stores the lock, including key and timestamp information, and is cleaned up when the commit is successful.

The job of a lock resolver is to respond to situations where a transaction is locked during submission.

When a transaction encounters a lock, there are several possible situations.

- The transaction to which Lock belongs has not submitted this key, and the Lock has not been cleaned up;
- The transaction to which Lock belongs has encountered an unrecoverable error, is being rolled back, and the Key has not been cleaned up;
- The node to which the lock belongs has an unexpected error, such as a node crashed, and the node to which the lock belongs is no longer able to update it.

Under the Percolator protocol, the state of the transaction is determined by querying the Primary Key to which the Lock belongs, but when we read an unfinished transaction (the Primary Key Lock has not been cleaned), what we expect is to wait for the submitted item to complete, and clean up the garbage data left by exceptions such as crashes. At this point, ttl is used to determine whether the transaction has expired, and when it encounters an overdue transaction, it will actively rollback it.

### TODO

The `getTxnStatus` and `resolveLock` functions are completed in `lock_resolver.go`, so that the exposed `ResolveLocks` function can function properly.

In addition to the transaction submission process, the transaction may also encounter a lock when reading data. At this time, the `ResolveLocks` function will also be triggered to complete the `tikvSnapshot.get` function in `snapshot.go`, so that the read request can run normally.

## Failpoint

[Failpoint](https://github.com/pingcap/failpoint) is a means of testing regular code paths through built-in error injection. Failpoint in Golang is implement using code generation, so in tests that use failpoint, you need to open failpoint first.

Open failpoint, the original code will be temporarily hidden to `{filename}__failpoint_stash__`, please do not delete it.

```sh
make failpoint-enable
```

Closing failpoint will recover the code in `{filename}__failpoint_stash__` to the original file and delete the stash file for temporary storage.

```sh
make failpoint-disable
```

Since the `.go` file code using failpoint will be different when the failpoint switch state is different, please submit the code with failpoint turned off.

## Tests

Run `make proj6` and pass all test cases.

You can test the specified single use case or multiple use cases using the following command.

```sh
go test {package path} -check.f ^{regex}$
# example
go test -timeout 5s ./store/tikv -check.f ^TestFailAfterPrimary$
```
