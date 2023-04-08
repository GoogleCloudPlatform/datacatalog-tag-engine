# Copyright 2020 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
#     Unless required by applicable law or agreed to in writing, software
#     distributed under the License is distributed on an "AS IS" BASIS,
#     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#     See the License for the specific language governing permissions and
#     limitations under the License.
#


from dataclasses import dataclass
from typing import Optional
from typing import List

from sql_translator.rfmt.blocks import LineBlock as LB
from sql_translator.rfmt.blocks import IndentBlock as IB
from sql_translator.rfmt.blocks import TextBlock as TB
from sql_translator.rfmt.blocks import StackBlock as SB
from sql_translator.rfmt.blocks import ChoiceBlock as CB
from sql_translator.rfmt.blocks import WrapBlock as WB

from sql_translator.sql_parser.utils import with_commas
from sql_translator.sql_parser.utils import comments_sqlf

from sql_translator.sql_parser.const import SQLNumber
from sql_translator.sql_parser.ident import SQLIdentifier

from sql_translator.sql_parser.node import SQLNode
from sql_translator.sql_parser.node import SQLNodeList
from sql_translator.sql_parser.expr import SQLExpr

from sql_translator.sql_parser.query import SQLQuery
from sql_translator.sql_parser.query import SQLAlias
from sql_translator.sql_parser.query import SQLTableSource


@dataclass(frozen=True)
class SQLSetOp(SQLQuery):
    op: str
    left: SQLNode
    right: SQLNode

    def sqlf(self, compact):
        return SB(
            [self.left.sqlf(compact), TB(self.op),
             self.right.sqlf(compact)])


@dataclass(frozen=True)
class SQLField(SQLNode):
    expr: SQLNode
    alias: Optional[SQLAlias]
    comments: Optional[List[str]]

    @staticmethod
    def parse(lex) -> 'SQLField':
        expr = SQLExpr.parse(lex)
        alias = SQLAlias.consume(lex)
        return SQLField(expr, alias, lex.get_comments())

    def sqlf(self, compact):
        comments_sql = comments_sqlf(self.comments)

        compact_sql = self.expr.sqlf(True)
        if self.alias:
            compact_sql = LB(
                [compact_sql, TB(' '), self.alias.sqlf(True)])
        if comments_sql:
            compact_sql = SB(comments_sql + [compact_sql])

        if compact:
            return compact_sql

        full_sql = self.expr.sqlf(False)
        if self.alias:
            full_sql = SB(
                comments_sql + [full_sql,
                                IB(self.alias.sqlf(True))])
        elif comments_sql:
            full_sql = SB(comments_sql + [full_sql])

        return CB([compact_sql, full_sql])


@dataclass(frozen=True)
class SQLOrderedColumn(SQLNode):
    col: SQLNode
    dir: Optional[str] = None

    def sqlf(self, compact):
        if self.dir:
            return LB(
                [self.col.sqlf(compact), TB(' ' + self.dir)])
        return self.col.sqlf(compact)


@dataclass(frozen=True)
class SQLOrderLimitOffset(SQLNode):
    order: SQLNodeList[SQLOrderedColumn]
    limit: Optional[SQLNumber] = None
    offset: Optional[SQLNumber] = None

    def sqlf(self, compact):
        lines = []
        if self.order:
            lines.append(TB(' ORDER BY '))
            lines.append(LB(with_commas(compact, self.order)))
        if self.limit:
            lines.append(TB(' LIMIT '))
            lines.append(TB(str(self.limit)))
        if self.offset:
            lines.append(TB(' OFFSET '))
            lines.append(TB(str(self.offset)))
        compact_sql = LB(lines)

        if compact:
            return compact_sql

        stack = []
        if self.order:
            stack.append(TB(' ORDER BY'))
            stack.append(
                IB(WB(with_commas(compact, self.order))))
        if self.limit:
            stack.append(TB(' LIMIT'))
            stack.append(IB(TB(str(self.limit))))
        if self.offset:
            stack.append(TB(' OFFSET'))
            stack.append(IB(TB(str(self.offset))))

        return CB([
            compact_sql,
            SB(stack)
        ])

    @staticmethod
    def consume(lex) -> 'Optional[SQLOrderLimitOffset]':
        orders = []
        if lex.consume('ORDER'):
            lex.expect('BY')
            while True:
                oexpr = SQLExpr.parse(lex)
                if lex.consume('ASC'):
                    orders.append(SQLOrderedColumn(oexpr, 'ASC'))
                elif lex.consume('DESC'):
                    orders.append(SQLOrderedColumn(oexpr, 'DESC'))
                else:
                    orders.append(SQLOrderedColumn(oexpr, None))
                if not lex.consume(','):
                    break

        limit = None
        offset = None
        if lex.consume('LIMIT'):
            limit = (SQLNumber.consume(lex) or
                     lex.error('Expected limit number'))
            if lex.consume('OFFSET'):
                offset = (SQLNumber.consume(lex) or
                          lex.error('Expected offset number'))

        if orders or limit or offset:
            return SQLOrderLimitOffset(SQLNodeList(orders), limit, offset)

        return None


@dataclass(frozen=True)
class SQLOrderedQuery(SQLQuery):
    query: SQLQuery
    ordering: SQLNode

    def sqlf(self, compact):
        # TODO(scannell) Wrap in braces if it's not a SQLSelect type

        compact_sql = LB(
            [self.query.sqlf(True), self.ordering.sqlf(True)])
        if compact:
            return compact_sql

        return CB([
            compact_sql,
            SB([
                self.query.sqlf(compact),
                self.ordering.sqlf(compact),
            ])
        ])


@dataclass(frozen=True)
class SQLSelect(SQLQuery):
    fields: SQLNodeList[SQLField]
    where_expr: Optional[SQLExpr] = None
    groups: Optional[SQLNodeList[SQLField]] = None
    from_tables: Optional[SQLNode] = None
    select_as_type: Optional[str] = None
    select_type: Optional[str] = None
    having_expr: Optional[SQLExpr] = None
    qualify_expr: Optional[SQLExpr] = None
    comments: Optional[List[str]] = None

    def sqlf(self, compact):

        select_clause = [TB('SELECT')]
        if self.select_as_type:
            select_clause.append(TB(' AS ' + self.select_as_type))
        if self.select_type:
            select_clause.append(TB(' ' + self.select_type))

        lines = []
        lines.append(LB(select_clause))
        lines.append(TB(' '))
        lines.append(LB(with_commas(True, self.fields)))
        if self.from_tables:
            lines.append(TB(' FROM '))
            lines.append(self.from_tables.sqlf(True))
        if self.where_expr:
            lines.append(TB(' WHERE '))
            lines.append(self.where_expr.sqlf(True))
        if self.groups:
            lines.append(TB(' GROUP BY '))
            lines.append(LB(with_commas(True, self.groups)))
        if self.having_expr:
            lines.append(TB(' HAVING '))
            lines.append(self.having_expr.sqlf(True))
        if self.qualify_expr:
            lines.append(TB(' QUALIFY '))
            lines.append(self.qualify_expr.sqlf(True))
        compact_sql = LB(lines)

        if compact:
            return compact_sql

        stack = comments_sqlf(self.comments)
        stack.append(LB(select_clause))
        stack.append(IB(SB(with_commas(compact, self.fields))))
        if self.from_tables:
            stack.append(TB('FROM'))
            stack.append(IB(self.from_tables.sqlf(compact)))
        if self.where_expr:
            stack.append(TB('WHERE'))
            stack.append(IB(self.where_expr.sqlf(compact)))
        if self.groups:
            stack.append(TB('GROUP BY'))
            stack.append(
                IB(WB(with_commas(compact, self.groups))))
        if self.having_expr:
            stack.append(TB('HAVING'))
            stack.append(IB(self.having_expr.sqlf(compact)))
        if self.qualify_expr:
            stack.append(TB('QUALIFY'))
            stack.append(IB(self.qualify_expr.sqlf(compact)))

        return CB([
            compact_sql,
            SB(stack)
        ])

    @staticmethod
    def parse(lex) -> 'SQLSelect':

        lex.expect('SELECT')

        comments = lex.get_comments()

        select_as_type = None
        if lex.consume('AS'):
            select_as_type = (lex.consume('STRUCT') or
                              lex.consume('VALUE') or
                              lex.error('Expected STRUCT or VALUE'))

        select_type = (lex.consume('DISTINCT') or
                       lex.consume('ALL') or
                       None)

        if not select_as_type and lex.consume('AS'):
            select_as_type = (lex.consume('STRUCT') or
                              lex.consume('VALUE') or
                              lex.error('Expected STRUCT or VALUE'))

        fields: List[SQLField] = []
        while True:
            fields.append(SQLField.parse(lex))
            if not lex.consume(','):
                break

        from_tables = lex.consume('FROM') and SQLFrom.parse(lex)
        where_expr = lex.consume('WHERE') and SQLExpr.parse(lex)

        groups: List[SQLField] = []
        if lex.consume('GROUP'):
            lex.expect('BY')
            while True:
                groups.append(SQLField.parse(lex))
                if not lex.consume(','):
                    break

        having_expr = lex.consume('HAVING') and SQLExpr.parse(lex)
        qualify_expr = lex.consume('QUALIFY') and SQLExpr.parse(lex)

        # TODO(scannell) Support WINDOW
        # Error on Window
        if lex.consume('WINDOW'):
            lex.error('WINDOW unsupported')

        comments.extend(lex.get_comments())

        return SQLSelect(SQLNodeList(fields), where_expr,
                         SQLNodeList(groups), from_tables,
                         select_as_type, select_type, having_expr,
                         qualify_expr, comments)


@dataclass(frozen=True)
class SQLSubSelect(SQLQuery):
    query: SQLQuery
    alias: Optional[SQLAlias] = None

    def sqlf(self, compact):
        stack = [
            TB('('), IB(self.query.sqlf(compact)),
            TB(')')
        ]
        if self.alias:
            stack.append(LB([TB(' '), self.alias.sqlf(True)]))
        return SB(stack)


@dataclass(frozen=True)
class SQLWithSelect(SQLQuery):
    tables: SQLNodeList[SQLIdentifier]
    sqls: SQLNodeList[SQLQuery]
    select: SQLNode

    def sqlf(self, compact):
        if not self.tables:
            return self.select.sqlf(compact)
        stack = [TB('WITH')]
        for i, table in enumerate(self.tables):
            iblock = [LB([table.sqlf(True), TB(' AS (')])]
            iblock.append(IB(self.sqls[i].sqlf(compact)))
            if i == len(self.tables) - 1:
                iblock.append(TB(')'))
            else:
                iblock.append(TB('),'))
            stack.append(IB(SB(iblock)))

        stack.append(self.select.sqlf(compact))

        return SB(stack)

    @staticmethod
    def parse(lex) -> 'SQLWithSelect':
        return (SQLWithSelect.consume(lex) or
                lex.error('Expected SELECT statement'))

    @staticmethod
    def consume(lex) -> 'Optional[SQLWithSelect]':
        tables: List[SQLIdentifier] = []
        sqls: List[SQLQuery] = []
        if lex.consume('WITH'):
            while True:
                tables.append(SQLIdentifier.parse(lex))
                lex.expect('AS')
                lex.expect('(')
                sqls.append(SQLQuery.parse(lex))
                lex.expect(')')
                if not lex.consume(','):
                    break

        query: SQLQuery
        if lex.peek('SELECT'):
            query = SQLSelect.parse(lex)
        elif not tables and (lex.peek(['(', 'WITH']) or
                             lex.peek(['(', 'SELECT'])):
            lex.consume('(')
            query = SQLSubSelect(SQLQuery.parse(lex), None)
            lex.expect(')')
        elif tables:
            lex.error('Expected SELECT or sub-query')
        else:
            return None

        order_limit_offset = SQLOrderLimitOffset.consume(lex)

        if order_limit_offset:
            query = SQLOrderedQuery(query, order_limit_offset)

        # These need to create a function that binds these things together
        if lex.consume('INTERSECT'):
            lex.expect('DISTINCT')
            query = SQLSetOp('INTERSECT DISTINCT', query,
                             SQLQuery.parse(lex))

        elif lex.consume('EXCEPT'):
            lex.expect('DISTINCT')
            query = SQLSetOp('EXCEPT DISTINCT', query,
                             SQLQuery.parse(lex))

        elif lex.consume('MINUS'):
            query = SQLSetOp('MINUS', query,
                             SQLQuery.parse(lex))

        elif lex.consume('UNION'):
            union_op = 'UNION'
            if lex.consume('ALL'):
                union_op += ' ALL'
            elif lex.consume('DISTINCT'):
                union_op += ' DISTINCT'
            query = SQLSetOp(union_op, query, SQLQuery.parse(lex))

        # Return query with orders, limit, bindings, etc
        return SQLWithSelect(SQLNodeList(tables), SQLNodeList(sqls), query)


@dataclass(frozen=True)
class SQLFrom(SQLNode):
    base: SQLNode
    joins: SQLNodeList[SQLNode]

    def sqlf(self, compact):
        fields = [self.base.sqlf(compact)]

        for j in self.joins:
            if compact:
                fields.append(TB(' '))
            fields.append(j.sqlf(compact))

        if compact:
            return LB(fields)

        return SB(fields)

    @staticmethod
    def parse(lex) -> 'SQLFrom':
        base_table = SQLTableSource.parse(lex)

        joins = []
        while True:
            join = SQLJoin.parse(lex)
            if not join:
                break
            joins.append(join)

        return SQLFrom(base_table, SQLNodeList(joins))


@dataclass(frozen=True)
class SQLJoin(SQLNode):
    join_type: str
    table: SQLNode
    join_expr: Optional[SQLNode]

    def sqlf(self, compact):
        join_type = TB(self.join_type + ' JOIN ')
        if self.join_expr is None:
            return LB([join_type, self.table.sqlf(compact)])
        compact_sql = LB([
            join_type, self.table.sqlf(True), TB(' ON '),
            self.join_expr.sqlf(True)
        ])
        if compact:
            return compact_sql
        return CB([
            compact_sql, SB([
                LB([
                    join_type,
                    self.table.sqlf(False),
                    TB(' ON')
                ]),
                IB(self.join_expr.sqlf(False))
            ])
        ])

    @staticmethod
    def parse_type(lex) -> Optional[str]:

        join_type = lex.consume_any(['INNER', 'CROSS'])
        if join_type:
            lex.expect('JOIN')
            return join_type

        join_type = lex.consume_any(['FULL', 'LEFT', 'RIGHT'])
        if join_type:
            if lex.consume('OUTER'):
                join_type = join_type + ' OUTER'
            lex.expect('JOIN')
            return join_type

        if lex.consume('JOIN'):
            return 'INNER'

        if lex.consume(','):
            return 'CROSS'

        return None

    @staticmethod
    def parse(lex) -> 'Optional[SQLJoin]':
        join_type = SQLJoin.parse_type(lex)
        if join_type is None:
            return None
        join_table = SQLTableSource.parse(lex)
        join_expr = None
        if lex.consume('ON'):
            join_expr = SQLExpr.parse(lex)
        return SQLJoin(join_type, join_table, join_expr)
