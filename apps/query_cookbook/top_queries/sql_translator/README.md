
# SQL Translator

## Overview

This package is a general-purpose SQL parser, translator, analyzer, and formatter.

Note: This is not an officially supported Google product.

It is intended to -
 * Provide an example of a comprehensive recursive descent parser using SQL.
 * Provide a template for SQL parser, rewriting, analysis, and formatting.

The dialect of SQL primarily based on [BigQuery Standard SQL](https://cloud.google.com/bigquery/docs/reference/standard-sql),
with some additional features.

## Why a recursive descent parser?

After many attempts at using LL and LR parsers over many years -- the common tools used today -- I've
switched to using exclusively hand-crafted recursive descent parsers.

Some good background of the challenges of parser/lexer tools can be found in
[this blog post](https://blog.reverberate.org/2013/09/ll-and-lr-in-context-why-parsing-tools.html).

It is also noteworthy that GCC and clang have both switched to hand-crafted recursive
descent paresrs. See [gcc](http://gcc.gnu.org/wiki/New_C_Parser) and [clang](http://clang.llvm.org/features.html).

This provides an example of a complex parser that, I hope, is easy to read, maintain, and extend. The only state
is maintained in lexer.py that provides the fundamental methods for finding identifiers, strings, and numbers.

## Potential Use Cases

There are many potential use cases for extending this code:

 * Providing a formatter tool as part of continuous integration.
 * Analyzing code for table dependencies and other interesting information. Search the AST for any information you'd like.
 * Translating SQL -- possibly custom SQL dialect -- into different target dialects.

Note that in all cases it will take extending the parser and formatting as needed. This is not officially supported Google product.

## Running examples

Please see [examples](EXAMPLES.md).

## Components

### sql_parser

sql_parser is a pure [recursive-descent parser](https://en.wikipedia.org/wiki/Recursive_descent_parser)
that generates Python frozen dataclass objects as an AST.

The AST also contains methods to generate SQL formatted by rfmt.

The data used:
 * Frozen Python Dataclass nodes as the abstract syntax tree.
 * Lexer that contains the string, list of comments (for association), and current position.

The only two variables modified is the parsing position and list of comments. The rest of the
parsing state is the current call-stack and execution point.

### sql_rewriter

sql_rewriter contains examples of of re-written the AST for different dialects.

Some examples include:
  * Converting CASE statements into IF() functions.
  * Converting colon cast to CAST() functions.

It also includes an extraction algorithm for detecting dependency between tables, and a
graph minimisation algorithm for producing a [graphviz](https://graphviz.org/) comaptible
graph.

### rfmt

rfmt is copied from [https://github.com/google/rfmt](https://github.com/google/rfmt/tree/master/inst/python/rfmt).

This is a general purpose formatting library. More information on its approach
can be found in the [research paper](https://research.google/pubs/pub44667/).

