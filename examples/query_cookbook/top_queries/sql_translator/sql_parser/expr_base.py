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
from typing import List
from typing import Optional

from sql_translator.rfmt.blocks import ChoiceBlock as CB
from sql_translator.rfmt.blocks import IndentBlock as IB
from sql_translator.rfmt.blocks import LineBlock as LB
from sql_translator.rfmt.blocks import StackBlock as SB
from sql_translator.rfmt.blocks import TextBlock as TB
from sql_translator.rfmt.blocks import WrapBlock as WB

from sql_translator.sql_parser.utils import with_commas

from sql_translator.sql_parser.query import SQLQuery
from sql_translator.sql_parser.query_impl import SQLOrderLimitOffset
from sql_translator.sql_parser.expr import SQLExpr
from sql_translator.sql_parser.const import SQLConstant
from sql_translator.sql_parser.const import SQLNumber
from sql_translator.sql_parser.ident import SQLIdentifierPath
from sql_translator.sql_parser.node import SQLNode
from sql_translator.sql_parser.node import SQLNodeList
from sql_translator.sql_parser.expr_funcs import SQLFuncExpr, SQLCustomFuncs


@dataclass(frozen=True)
class SQLArrayLiteral(SQLExpr):
    args: SQLNodeList

    def sqlf(self, compact):
        compact_sql = LB([
            TB('['),
            LB(with_commas(True, self.args)),
            TB(']')
        ])
        if compact:
            return compact_sql
        return CB([
            compact_sql,
            LB([
                TB('['),
                WB(with_commas(compact, self.args, tail=']'))
            ]),
        ])

    @staticmethod
    def consume(lex) -> 'Optional[SQLArrayLiteral]':
        if not lex.consume('['):
            return None
        exprs: List[SQLExpr] = []
        while True:
            exprs.append(SQLExpr.parse(lex))
            if not lex.consume(','):
                break
        lex.expect(']')
        return SQLArrayLiteral(SQLNodeList(exprs))


@dataclass(frozen=True)
class SQLArraySelect(SQLExpr):
    query: SQLQuery

    def sqlf(self, compact):
        compact_sql = LB([
            TB('ARRAY('), self.query.sqlf(True), TB(')')
        ])

        if compact:
            return compact_sql

        return CB([
            compact_sql,
            SB([
                TB('ARRAY('),
                IB(self.query.sqlf(compact)),
                TB(')')
            ])
        ])

    @staticmethod
    def consume(lex) -> 'Optional[SQLArraySelect]':
        if not lex.consume('ARRAY'):
            return None

        lex.expect('(')
        query = SQLQuery.parse(lex)
        lex.expect(')')
        return SQLArraySelect(query)


@dataclass(frozen=True)
class SQLArrayAgg(SQLExpr):
    is_distinct: bool
    expr: SQLNode
    nulls: Optional[str]
    order_limit_offset: Optional[SQLOrderLimitOffset]
    analytic: Optional[SQLNode]

    def sqlf(self, compact):
        lines = [TB('ARRAY_AGG(')]
        if self.is_distinct:
            lines.append(TB('DISTINCT '))
        lines.append(self.expr.sqlf(True))
        if self.nulls:
            lines.append(TB(self.nulls) + ' NULLS')
        if self.order_limit_offset:
            lines.append(self.order_limit_offset.sqlf(True))
        if self.analytic:
            lines.append(self.analytic.sqlf(True))
        lines.append(TB(')'))

        compact_sql = LB(lines)

        if compact:
            return compact_sql

        stack = [TB('ARRAY_AGG(')]
        indent = []
        if self.is_distinct:
            indent.append(
                LB([TB('DISTINCT '), self.expr.sqlf(compact)]))
        else:
            indent.append(self.expr.sqlf(compact))
        if self.nulls:
            indent.append(TB(self.nulls) + ' NULLS')
        if self.order_limit_offset:
            indent.append(self.order_limit_offset.sqlf(compact))
        if self.analytic:
            indent.append(self.analytic.sqlf(compact))
        stack.append(IB(SB(indent)))
        stack.append(TB(')'))

        return CB([
            compact_sql,
            SB(stack)
        ])

    @staticmethod
    def consume(lex) -> 'Optional[SQLArrayAgg]':
        if not lex.consume('ARRAY_AGG'):
            return None

        lex.expect('(')

        is_distinct = bool(lex.consume('DISTINCT'))

        expr = SQLExpr.parse(lex)

        nulls = None
        if lex.consume('IGNORE'):
            nulls = 'IGNORE'
            lex.expect('NULLS')
        elif lex.consume('RESPECT'):
            nulls = 'RESPECT'
            lex.expect('NULLS')

        order_limit_offset = SQLOrderLimitOffset.consume(lex)

        analytic = SQLAnalytic.consume(lex)

        lex.expect(')')

        return SQLArrayAgg(is_distinct, expr, nulls,
                           order_limit_offset, analytic)


@dataclass(frozen=True)
class SQLExprWithAnalytic(SQLExpr):
    function: SQLExpr
    analytic: SQLNode

    def sqlf(self, compact):
        compact_sqlf = LB([self.function.sqlf(compact), TB(' '),
                           self.analytic.sqlf(compact)])
        if compact:
            return compact_sqlf

        return CB([
            compact_sqlf,
            SB([self.function.sqlf(compact),
                self.analytic.sqlf(compact)])
        ])

    @staticmethod
    def parse(lex) -> 'SQLExpr':

        # Try alternatives first
        expr: SQLExpr = (SQLConstant.consume(lex) or
                         SQLArrayLiteral.consume(lex) or
                         SQLArrayAgg.consume(lex) or
                         SQLArraySelect.consume(lex) or
                         SQLCustomFuncs.consume(lex) or
                         SQLIdentifierPath.parse(lex))

        # If it is a SQLIdentifierPath, it may be
        # a normal function call.
        if isinstance(expr, SQLIdentifierPath) and lex.consume('('):

            # Parse as a function
            func_args: List[SQLExpr] = []
            if not lex.consume(')'):
                while True:
                    func_args.append(SQLExpr.parse(lex))
                    if not lex.consume(','):
                        break
                lex.expect(')')

            # Turn it into a function
            expr = SQLFuncExpr(expr.names, SQLNodeList(func_args))

        window = SQLAnalytic.consume(lex)
        if window:
            expr = SQLExprWithAnalytic(expr, window)

        return expr


@dataclass(frozen=True)
class SQLAnalytic(SQLExpr):
    partition_by: SQLNodeList
    order_by: SQLNodeList
    range_desc: str

    def sqlf(self, compact):
        lines = []
        lines.append(TB('OVER ('))
        if self.partition_by:
            lines.append(TB('PARTITION BY '))
            lines.extend(with_commas(True, self.partition_by, ' '))
        if self.order_by:
            lines.append(TB('ORDER BY '))
            lines.extend(with_commas(True, self.order_by, ' '))
        if self.range_desc:
            lines.append(TB(self.range_desc))
        lines.append(TB(')'))

        if compact:
            return LB(lines)

        full_sql = [
            TB('OVER ('),
        ]
        if self.partition_by:
            full_sql.append(
                IB(
                    SB([
                        TB('PARTITION BY'), IB(
                            WB(with_commas(True, self.partition_by)))
                    ])))
        if self.order_by:
            full_sql.append(
                IB(
                    SB([
                        TB('ORDER BY'), IB(
                            WB(with_commas(True, self.order_by)))
                    ])))
        if self.range_desc:
            full_sql.append(IB(TB(self.range_desc)))
        full_sql.append(TB(')'))

        r = CB([LB(lines), SB(full_sql)])
        return r

    @staticmethod
    def consume(lex) -> 'Optional[SQLAnalytic]':
        if not lex.consume('OVER'):
            return None

        lex.expect('(')

        partition_by = []
        if lex.consume('PARTITION'):
            lex.expect('BY')
            while True:
                partition_by.append(SQLExpr.parse(lex))
                if not lex.consume(','):
                    break

        order_by = []
        if lex.consume('ORDER'):
            lex.expect('BY')
            while True:
                oby = SQLExpr.parse(lex)
                order = None
                if lex.consume('ASC'):
                    order = 'ASC'
                elif lex.consume('DESC'):
                    order = 'DESC'
                # TODO(scannell): Capture ASC/DESC
                order_by.append(oby)
                if not lex.consume(','):
                    break

        win_spec = (lex.consume('ROWS') or lex.consume('RANGE'))

        # If window specified, parse it out
        if win_spec:
            if lex.consume('BETWEEN'):
                win_spec += (' BETWEEN ' +
                             SQLAnalytic._parse_frame_boundary(lex))
                lex.expect('AND')
                win_spec += ' AND ' + SQLAnalytic._parse_frame_boundary(lex)
            else:
                win_spec += ' ' + SQLAnalytic._parse_frame_boundary(lex)

        lex.expect(')')

        # Return analytics function
        return SQLAnalytic(SQLNodeList(partition_by),
                           SQLNodeList(order_by),
                           win_spec)

    @staticmethod
    def _parse_frame_boundary(lex):
        if lex.consume('UNBOUNDED'):
            lex.expect('PRECEDING')
            return 'UNBOUNDED PRECEDING'

        if lex.consume('CURRENT'):
            lex.expect('ROW')
            return 'CURRENT ROW'

        num = SQLNumber.consume(lex)

        num_typ = (lex.consume('PRECEDING') or lex.consume('FOLLOWING') or
                   lex.error('Expected PRECEDING or FOLLOWING'))

        return '{} {}'.format(num, num_typ)
