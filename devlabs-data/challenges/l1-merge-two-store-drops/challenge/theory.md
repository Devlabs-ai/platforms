# Union vs unionByName

## unionByName (this lab)
Aligns columns **by name**, not position. Safer when schemas evolve.

```python
east = spark.read.parquet(INPUT_A_PATH)
west = spark.read.parquet(INPUT_B_PATH)
out = east.unionByName(west)
```

## union
Aligns by **position** — dangerous if column order differs.

```python
east.union(west)  # positional
```

## allowMissingColumns
`unionByName(other, allowMissingColumns=True)` fills missing cols with null — not required here (schemas match).
