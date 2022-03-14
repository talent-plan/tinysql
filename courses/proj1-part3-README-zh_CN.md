# 关系代数模型

## 关系

等同于我们常说的表(table)的概念，是一个二维的行列结构。

列的取值范围被称为"域"(domain)，代表一个集合$D$，按不同类型划分有不同的取值范围，比如32位整数范围$[-2^31,2^31-1]$等等。

将域用笛卡尔积串联起来，形成一个$n$元组的集合，每个元组代表一行：
$$ D_1 \times D_2 \times \dots \times D_n = \{(d_1,d_2,\dots,d_n)|d_i \in D_i, i = 1,2,\dots,n\}$$

从这些所有可能的$n$元组中取出一部分来，即形成关系。使用关系模式(schema)来定义关系$R(A_1:D_1,A_2:D_2,\dots,A_n:D_n)$，一般会为关系定义一个关系名，$A_1,A_2,\dots,A_n$即为关系中的列1、列2...列n，$D_1,D_2,\dots,D_n$即为对应列的取值范围。

还可以为关系或列定义约束，如非空约束等等, 创建关系对应于SQL中的语句：

```sql
CREATE TABLE table_name (column_name_1 data_type_1 constraint, column_name_2 data_type_2 constraint,...);
```

例如教程中提到的

```sql
CREATE TABLE person (
    number INT(11) PRIMARY KEY,
    name VARCHAR(255),
    region VARCHAR(255),
    birthday DATE
);
```
## 关系运算

### 1. 选择
给定一个关系$R$，按照某种条件$condition$进行筛选，结果仍然构成一个关系。
$$
\sigma_{con}(R)=\{t|t\in R \land con(t) = TRUE\}
$$

其中$condition$由比较表达式$X \theta Y$组成，$X$和$Y$可以由具体的操作数(比如1,3.3,"aaa")组成，也可以由元组某个属性，如$t[id]$组成。$\theta$可以是大于小于等于不等于之类的比较运算符。比较表达式可以用交（$and$）并（$or$）或非（$not$）串联起来。

我们常见的$select$语句就对应这种选择运算。

```sql
    SELECT * FROM R WHERE condition;
```

### 2. 投影
给定一个关系$R$，投影运算结果也是一个关系，记作$\Pi_A(R)$，它从关系R中选出包含在$A$中的列构成，$A$必须是关系$R$的子集：

$$
\Pi_{A_1,A_2,\dots,A_n}(R) = \{<t[A_1],t[A_2],\dots,t[A_n]>|t \in R\}
$$

对应$select$后面的那一部分，星号$*$表示选择全部列：

```sql
    SELECT A1,A2,...,An FROM R WHERE condition;
```

### 3. 广义笛卡尔积
关系$R$与关系$S$的广义笛卡尔积（也叫广义积，积）运算结果也是一个关系，记作$R \times S$，它由关系$R$中的元组与关系$S$中的元组进行所有可能的拼接构成。

$$
R \times S = \{<a_1,a_2,\dots,a_n,b_1,b_2,\dots,b_n> |<a_1,a_2,\dots,a_n> \in R \land <b_1,b_2,\dots,b_n> \in S \}
$$

对应$from$后面的那一部分：

```sql
    SELECT A1,A2,...,An FROM R1,R2,...,Rn WHERE condition;
```