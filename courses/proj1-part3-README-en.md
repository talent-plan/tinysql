# Relational Algebraic Model

## Relation

Equivalent to the concept of table that we often say, a relation is a two-dimensional row/column structure.

The value range for a column is called a "domain", representing a set of $D$. There are different value ranges for different types, such as the 32-bit integer range $ [-2^31,2^31-1] $, etc.

The domains are concatenated using a Cartesian product to form a set of $n$ tuples, each representing a line:
$$ D*1\ times D*2\ times\ dots\ times D*n =\ {(d*1, d*2,\ dots, d*n) |d*i\ in D*i, i = 1,2,\ dots, n\} $$

Take a portion of all of these possible $n$ tuples to form a relation. The relation $R (A*1: D*1, A*2: D*2,\ dots, A*n:D*n) $ is used to define the relation. Generally, a relation will be given a name. $A*1, A*2,\ dots, A*n$ is column 1, column 2 in the relation... column n, $D*1, d*2,\ dots, D*n$ is the range of values for the corresponding column.

You can also define constraints for a relation or column, such as a non-null constraint, etc., to create a relation corresponding to a statement in SQL:

```sql
CREATE TABLE table_name (column_name_1 data_type_1 constraint, column_name_2 data_type_2 constraint,...);
```

For example, the one mentioned in the tutorial

```sql
CREATE TABLE person (
    number INT(11) PRIMARY KEY,
    name VARCHAR(255),
    region VARCHAR(255),
    birthday DATE
);
```
## Relational Arithmetic

### 1. selects
Given a relation $R$, filter $condition$ according to certain conditions, and the result still forms a relation.
$$
\ sigma_ {con} (R) =\ {t|t\ in R\ land con (t) = TRUE\}
$$

Where $condition$ consists of the comparison expression $X\ theta Y$, $X$ and $Y$ can be composed of specific operands (such as 1,3.3, "aaa"), or a tuple property, such as $t [id] $. $\ theta$ can be a comparison operator greater than or less than equal to not equal. Comparison expressions can be concatenated using ($and$) and ($or$) or not ($not$).

Our common $select$ statement corresponds to this kind of selection operation.

```sql
    SELECT * FROM R WHERE condition;
```

### 2. projection
Given a relation $R$, the projection result is also a relation, recorded as $\ Pi_A (R) $. It selects the column components contained in $A$ from the relation R, and $A$ must be a subset of the relation $R$:

$$
\ pi*{A*1, A*2,\ dots, A*n} (R) =\ |t <t [A*1], t [A*2],\ dots, t [A_n] >\ in R\}
$$

Corresponding to the part after $select$, the asterisk $*$ indicates that all columns are selected:

```sql
    SELECT A1,A2,...,An FROM R WHERE condition;
```

### 3. Generalized Cartesian product
The result of the calculation of the relation $R$ and the Generalized Cartesian product (also known as the general product, product) of the relation $S$ is also a relation. It is recorded as $R\ times S$. It consists of all possible splicing of the tuples in the relation $R$ and the tuples in the relation $S$.

$$
R\ times S =\ {<a*1, a*2,\ dots, a*n, b*1, b*2,\ dots, b*n> |<a*1, a*2,\ dots, a*n>\ in R\ land <b*1, b*2,\ dots, b*n>\ in S\}
$$

Corresponds to the part after $from$:

```sql
    SELECT A1,A2,...,An FROM R1,R2,...,Rn WHERE condition;
```
