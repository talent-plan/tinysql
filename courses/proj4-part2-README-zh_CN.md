# Join and Access Path Selection

## 概述	

在这一部分，我们会需要实现和代价选择相关的一些内容。

## 描述数据分布的数据结构

当结束启发式规则的筛选之后，我们仍然可能剩余多组索引等待筛选我们就需要知道每个索引究竟会过滤多少行数据。在 TiDB 中我们使用直方图和 Count-Min Sketch 来存储的统计信息，[TiDB 源码阅读系列文章（十二）统计信息（上）](https://pingcap.com/blog-cn/tidb-source-code-reading-12/) 中，我们对直方图和 Count-Min Sketch 的实现原理做了比较详细的介绍。

这里我们需要完成 `cmsketch.go` 中的 TODO 内容，并通过 `cmsketch_test.go` 中的测试

## Join Reorder

如果我们不加修改的直接执行用户输入的 Join 的顺序，假设用户输入了 `select * from t1 joni t2 on ... join t3 on ...`，我们会按照先扫描 `t1`，然后和 `t2` 做 Join，然后和 `t3` 做 Join。这个顺序会在很多情况下表现的并不是特别好（一个简单的场景，`t1`, `t2` 特别大，而 `t3` 特别小，那么只要和 `t3` join 时可以过滤部分数据，先 Join `t1` 和 `t2` 势必没有先让 `t3` 和其中一个表 join 在性能上占优势）。

你需要实现一个基于 DP 的 Join Reorder 算法，并通过 `rule_join_reorder_dp_test.go` 中的测试。实现的位置在文件 `rule_join_reorder_dp.go` 中。

这里我们简单的表述一下这个算法：

- 使用数字的二进制表示来代表当前参与 Join 的节点情况。11（二进制表示为 1011）表示当前的 Join Group 包含了第 3 号节点，第 1 号节点和第 0 号节点（节点从 0 开始计数）。
- f[11] 来表示包含了节点 `3, 1, 0` 的最优的 Join Tree。
- 转移方程则是 `f[group] = min{join{f[sub], f[group ^ sub])}` 这里 `sub` 是 `group` 二进制表示下的任意子集。


## Access Path Selection

在实际场景中，一个表可能会有多个索引，一个查询中的查询条件也可能涉及到某个表的多个索引。因此就会需要我们去决定选择使用哪个索引。你会需要实现这个过程的某些代码

### Skyline pruning

你可以在 [TiDB proposal](https://github.com/pingcap/tidb/blob/master/docs/design/2019-01-25-skyline-pruning.md) 以及 [Skyline pruning operator](http://skylineresearch.in/skylineintro/The_Skyline_Operator.pdf) 来了解其理论基础。

这是一个启发式规则的筛选，用来筛除一些一定会差的选择分支。具体的筛选要求在 TiDB proposal 以及 `TODO` 注释的解释中有更详细的说明。你需要实现并通过 `TestSkylinePruning` 中的所有测试。实现的位置为 `find_best_task.go` 的 TODO 内容。


