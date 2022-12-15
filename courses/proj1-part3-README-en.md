# Section III Relational Model

The relational model uses a collection of tables to represent
both data and the relationships among those data. Each table has multiple
columns, and each column has a unique name. Tables are also known
as relations.

For each attribute of a relation, there is a set of permitted values, called the
`domain` of that attribute.

## Relational Algebra

### 1. Selection

Given a relation **R**, filter **condition** according to certain conditions, and the result still forms a relation. In other words, selection return rows of the input relation that satisfy the predicate.

Here is a sample selection query:
```sql
SELECT * FROM R WHERE condition;
```

### 2. Projection
A projection outputs specified attributes from all rows of the input relation. It removes duplicate tuples from the output.

```sql
SELECT A1,A2,...,An FROM R WHERE condition;
```

### 3. Cartesian product
Cartesian product outputs all pairs of rows from the two input relation (regardless of whether or not they have the same values on common attributes).

Corresponds to the part after $from$:

```sql
SELECT A1,A2,...,An FROM R1,R2,...,Rn WHERE condition;
```
