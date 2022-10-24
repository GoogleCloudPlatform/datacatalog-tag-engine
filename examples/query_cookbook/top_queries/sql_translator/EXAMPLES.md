# Examples

## SQL Rewriter

```
./sql.py --convert CLEANUP --type format examples/case.sql
```

## SQL Formatting

```
./sql.py --type format examples/i.sql
```

## SQL Table Analysis

### As a normal graph

```
./sql.py --type graph examples/table_analysis/*.sql > full_graph.dot
dot full_graph.dot -Tpdf > full_graph.pdf
```

See [full_graph.pdf](examples/table_analysis/full_graph.pdf)

### As a minimum graph

```
./sql.py --graph_minimise --type graph examples/table_analysis/*.sql > minimum_graph.dot
dot minimum_graph.dot -Tpdf > minimum_graph.pdf
```

See [minimum_graph.pdf](examples/table_analysis/minimum_graph.pdf)

