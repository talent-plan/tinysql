# SQL 语法

## 概览

在这一部分我们将介绍利用 SQL 在逻辑上表示以及处理数据。你可以在阅读的同时使用 [A Tour of TiDB](https://tour.pingcap.com/) 来动手操作数据库。 

## SQL 语法

对于数据库来说，关键的问题可能就是如何表示数据以及如何处理这些数据了。在关系型数据库中，数据是以“表”的形式存在的，例如在  [A Tour of TiDB](https://tour.pingcap.com/) 中，有一个 person 表：

|number|name|region|birthday|
|------:|----:|------:|--------:|
|1|tom|north|2019-10-27|
|2|bob|west|2018-10-27|
|3|jay|north|2018-10-25|
|4|jerry|north|2018-10-23|

该表一共有 4 行，每行有 4 列信息，这样每一行就成为了一个最小的完整信息单元，利用这样的一个或多个表，我们便可以在上面做出各种操作以得出想要的信息。

在表上最简单的操作便是直接输出全部行了，例如：

```sql
TiDB> select * from person;
+--------+-------+--------+------------+
| number | name  | region | birthday   |
+--------+-------+--------+------------+
| 1      | tom   | north  | 2019-10-27 |
| 2      | bob   | west   | 2018-10-27 |
| 3      | jay   | north  | 2018-10-25 |
| 4      | jerry | north  | 2018-10-23 |
+--------+-------+--------+------------+
4 row in set (0.00 sec)
```

还可以指定只输出需要的列，例如：

```sql
TiDB> select number,name from person;
+--------+-------+
| number | name  |
+--------+-------+
| 1      | tom   |
| 2      | bob   |
| 3      | jay   |
| 4      | jerry |
+--------+-------+
4 row in set (0.01 sec)
```

当然，有的时候可能我们只对满足某些条件的行感兴趣，例如我们可能只关心位于 north 的人：

```sql
TiDB> select name, birthday from person where region = 'north';
+-------+------------+
| name  | birthday   | 
+-------+------------+
| tom   | 2019-10-27 | 
| jay   | 2018-10-25 | 
| jerry | 2018-10-23 | 
+-------+------------+
3 row in set (0.01 sec)
```

通过 where 语句以及各种条件的组合，我们可以只得到满足某些信息的行。

有些时候，我们还需要一些概括性的数据，例如表里满足某个条件的一共有多少行，这个时候我们需要聚合函数来统计概括后的信息：


```sql
TiDB> select count(*) from person where region = 'north';
+----------+
| count(*) | 
+----------+
| 3        | 
+----------+
1 row in set (0.01 sec)
```

常见的聚合有 max，min，sum 和 count 等。上面的语句只是输出了满足 region = ‘north’ 的行数，如果我们同时也想知道其他所有 region 的总人数呢？ 此时 `group by`就排上用场了：



```sql
TiDB> select region, count(*) from person group by region;
+--------+----------+
| region | count(*) |
+--------+----------+
| north  | 3        |
| west   | 1        |
+--------+----------+
2 row in set (0.01 sec)
```

当然，对于聚合的结果我们可能还是需要过滤一些行，不过此时前面介绍的 where 语句就不能使用了，因为 where 后面的过滤条件是在 group by 之前生效的，在 group by 之后过滤需要使用 having:

```
TiDB> select region, count(*) from person group by region having count(*) > 1;
+--------+----------+
| region | count(*) | 
+--------+----------+
| north  | 3        | 
+--------+----------+
1 row in set (0.02 sec)
```

此外，还有一些常见的操作例如 order by，limit 等，这里不再一一介绍。
除了单表上的操作，往往我们可能需要结合多个表的信息，例如还有另外一张 address 表：

```sql
TiDB> create table address(number int, address varchar(50));
Execute success (0.05 sec)
TiDB> insert into address values (1, 'a'),(2, 'b'),(3, 'c'), (4, 'd');
Execute success (0.02 sec)
```

最简单的结合两张表的信息便是将分别取两张表中的任意一行结合起来，这样我们一共会有 4*4 种可能的组合，也就是会得到 16 行：

```sql
TiDB> select name, address from person inner join address;
+-------+---------+
| name  | address |
+-------+---------+
| tom   | a       |
| tom   | b       |
| tom   | c       |
| tom   | d       |
| bob   | a       |
| bob   | b       |
| bob   | c       |
| bob   | d       |
| jay   | a       |
| jay   | b       |
| jay   | c       |
| jay   | d       |
| jerry | a       |
| jerry | b       |
| jerry | c       |
| jerry | d       |
+-------+---------+
16 row in set (0.02 sec)
```

但这样的信息产生的信息爆炸往往是我们不需要的，幸运的是我们可以指定组合任意行的策略，例如如果想要同时知道某个人的地址以及名字，那我们只需要取两张表中有相同 number 值的人接合在一起，这样只会产生 4 行结果：

```sql
TiDB> select name, address from person inner join address on person.number = address.number;
+-------+---------+
| name  | address |
+-------+---------+
| tom   | a       |
| bob   | b       |
| jay   | c       |
| jerry | d       |
+-------+---------+
4 row in set (0.02 sec)
```

需要注意的是这里的 join 为 inner join，除此以外还有 outer join，在这里就不再赘述。


## 作业描述

完成 hackerrank 的下列题目

[Revising the Select Query I](https://www.hackerrank.com/challenges/revising-the-select-query/problem) (10%)
[Revising the Select Query II](https://www.hackerrank.com/challenges/revising-the-select-query-2/problem) (10%)
[Revising Aggregations - The Count Function](https://www.hackerrank.com/challenges/revising-aggregations-the-count-function/problem) (10%)
[Revising Aggregations - The Sum Function](https://www.hackerrank.com/challenges/revising-aggregations-sum/problem) (10%)
[Revising Aggregations - Averages](https://www.hackerrank.com/challenges/revising-aggregations-the-average-function/problem) (10%)
[African Cities](https://www.hackerrank.com/challenges/african-cities/problem) (10%)
[Average Population of Each Continent](https://www.hackerrank.com/challenges/average-population-of-each-continent/problem) (10%)
[Binary Tree Nodes](https://www.hackerrank.com/challenges/binary-search-tree-1/problem) (30%)

## 评分

该作业目前视为课下作业，暂时不计入评分。
