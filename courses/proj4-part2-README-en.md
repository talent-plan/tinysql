# Join and Access Path Selection

## Overview

In this part, we will implement some content related to cost selection.

## A data structure that describes the distribution of data.

After the heuristic rule selection is completed, we may still have multiple indexes remaining to be filtered, and we need to know how many rows of data each index will filter. In TiDB, we use histograms and Count-Min Sketch to store statistical information.

Here we need to complete the TODO content in `cmsketch.go` and pass the tests in `cmsketch_test.go`

## Join Reordering

Usually, we can not follow the join order according to the user input. Here is an example, assuming a user inputs `select * from t1 join t2 on ... join t3 on ...`, we will scan `t1` first, then do a join with `t2`, and then do a join with `t3`. This order does not perform well in many cases. For example, if `t1` and `t2` are very large and `t3` is very small, then joining `t3` with one of the tables first would have an advantage in performance, because it could filter out some data before doing the join with `t1` and `t2`.

You need to implement a DP-based Join Reorder algorithm and pass the tests in `rule_join_reorder_dp_test.go`. The implementation can be found in the file `rule_join_reorder_dp.go`.

Here's a brief description of this algorithm:

- Use binary representation of numbers to represent the current join nodes. 11 (1011 in binary) indicates that the current join group contains node 3, node 1, and node 0 (nodes are numbered starting from 0).
- f[11] to represent an optimal Join Tree containing node `3, 1, 0`.
- The transition equation is `f[group] = min{join{f[sub], f[group ^ sub])}`, where `sub` is any subset of the binary representation of `group`.


## Access Path Selection

In actual scenarios, a table may have multiple indexes, and the query conditions in a query may also involve multiple indexes of a table. Therefore, we need to decide which index to use. You will need to implement some code for this process.

### Skyline pruning

You can learn about its theoretical foundation in the [TiDB proposal](https://github.com/pingcap/tidb/blob/master/docs/design/2019-01-25-skyline-pruning.md) and [Skyline Pruning Operator](http://www.cs.ust.hk/~dimitris/PAPERS/SIGMOD03-Skyline.pdf).

This is a heuristic rule filtering that is used to eliminate some of the poor choices of branching. The specific filtering requirements are explained in more detail in the `TiDB proposal` and the TODO comments. You need to implement and pass all the tests in `TestSkylinePruning`. The implementation can be found in the TODO content in `find_best_task.go`.

## Tasks

Implement and pass all the tests in `TestSkylinePruning`.

## Rating

Pass all the tests in `TestSkylinePruning`
