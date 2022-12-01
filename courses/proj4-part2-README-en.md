# Join and Access Path Selection

## Overview

In this section, we'll need to implement some content related to cost options.

## A Data Structure Describing the Distribution of Data

After the heuristic filtering is finished, we may still have multiple sets of indexes left waiting to be filtered. We need to know exactly how many rows of data each index will filter. In TiDB, we use histograms and Count-Min Sketch to store statistical information. In [Read the TiDB source code series of articles (12) Statistics (Part 1)](https://pingcap.com/blog-cn/tidb-source-code-reading-12/), we have introduced the implement principles of histograms and Count-Min Sketch in more detail.

Here we need to complete the TODO content in `cmsketch.go` and pass the tests in `cmsketch_test.go`

## Join Reorder

If we execute the join order entered by the user directly without modification, assuming the user enters `select * from t1 joni t2 on ... join t3 on ...`, we will scan `t1` first, then join with `t2`, and then join with `t3`. This order doesn't perform particularly well in many cases (in a simple scenario, `t1`, `t2` is particularly large, and `t3` is very small, so as long as you can filter some data when joining `t3`, joining `t1` and `t2` will not have given a performance advantage.

You need to implement a DP-based Join Reorder algorithm and pass the tests in `rule_join_reorder_dp_test.go`. The location of the implement is in file `rule_join_reorder_dp.go`.

Here's a brief description of this algorithm:

- The binary representation of the number is used to represent the current state of the nodes participating in the join. 11 (1011 in binary) indicates that the current Join Group contains node 3, node 1, and node 0 (nodes start counting from 0).
- f [11] to represent an optimal Join Tree containing node `3, 1, 0`.
- The transfer equation is `f[group] = min{join{f[sub], f[group ^ sub])}`, where `sub` is any subset of the binary representation of `group`.


## Access Path Selection

In actual scenario, a table may have multiple indexes, and the query conditions in a query may also involve multiple indexes on a certain table. So it's up to us to decide which index to use. You'll need some code to implement this process

### Skyline pruning

You can read about the rationale behind [TiDB proposal](https://github.com/pingcap/tidb/blob/master/docs/design/2019-01-25-skyline-pruning.md) and [Skyline Pruning Operator](http://www.cs.ust.hk/~dimitris/PAPERS/SIGMOD03-Skyline.pdf).

This is a heuristic rule screening to filter out some branches that are bound to be poor choices. The specific screening requirements are explained in more detail in the TiDB proposal and in the explanation of the `TODO` comments. You'll need to implement and pass all of the tests in `TestSkylinePruning`. The implement location is the TODO content of `find_best_task.go`.
