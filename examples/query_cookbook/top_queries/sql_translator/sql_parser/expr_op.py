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

from sql_translator.rfmt.blocks import LineBlock as LB
from sql_translator.rfmt.blocks import TextBlock as TB
from sql_translator.rfmt.blocks import StackBlock as SB
from sql_translator.rfmt.blocks import ChoiceBlock as CB
from sql_translator.rfmt.blocks import IndentBlock as IB
from sql_translator.rfmt.blocks import WrapBlock as WB

from sql_translator.sql_parser.utils import with_commas

from sql_translator.sql_parser.node import SQLNodeList
from sql_translator.sql_parser.node import SQLNode
from sql_translator.sql_parser.query import SQLQuery
from sql_translator.sql_parser.types import SQLType
from sql_translator.sql_parser.expr import SQLExpr
from sql_translator.sql_parser.expr import SQLBaseExpr


@dataclass(frozen=True)
class SQLBiOp(SQLExpr):
    sql_op: str
    left: SQLExpr
    right: SQLExpr

    def sqlf(self, compact):
        right_expr_compact = self.right.sqlf(True)
        left_expr_compact = self.left.sqlf(True)

        compact_sql = LB([
            left_expr_compact, TB(' '), TB(self.sql_op),
            TB(' '),
            right_expr_compact
        ])
        if compact:
            return compact_sql

        right_expr = self.right.sqlf(False)
        left_expr = self.left.sqlf(False)

        return CB([
            compact_sql, SB([
                LB([left_expr, TB(' '), TB(self.sql_op)]),
                right_expr
            ])
        ])

    @staticmethod
    def parse(lex):
        return SQLBiOp.parse_or_expression(lex)

    @staticmethod
    def parse_or_expression(lex):
        expr = SQLBiOp.parse_and_expression(lex)

        while lex.consume('OR'):
            expr = SQLBiOp('OR', expr, SQLBiOp.parse_and_expression(lex))

        return expr

    @staticmethod
    def parse_and_expression(lex):
        expr = SQLBiOp.parse_not_expression(lex)

        while lex.consume('AND'):
            expr = SQLBiOp('AND', expr, SQLBiOp.parse_not_expression(lex))

        return expr

    @staticmethod
    def parse_not_expression(lex):
        # Add while loop here.
        if lex.consume('NOT'):
            return SQLUniOp('NOT', SQLBiOp.parse_compare_expression(lex))

        return SQLBiOp.parse_compare_expression(lex)

    @staticmethod
    def consume_basic_cmp_operator(lex):
        return lex.consume_any(['!=', '<>', '<=>', '<=',
                                '>=', '=', '<', '>'])

    @staticmethod
    def consume_is_operator(lex):
        if not lex.consume('IS'):
            return None

        is_op = 'IS '
        if lex.consume('NOT'):
            is_op += 'NOT '

        typ = lex.consume_any(['TRUE', 'FALSE', 'NULL'])
        if typ:
            return is_op+typ

        if lex.consume('DISTINCT'):
            lex.expect('FROM')
            return is_op + ' DISTINCT FROM'

        lex.error('Expected TRUE, FALSE, NULL, or DISTINCT FROM')
        return None

    @staticmethod
    def parse_compare_expression(lex):
        expr = SQLBiOp.parse_bit_or_expression(lex)

        while True:
            sql_op = SQLBiOp.consume_basic_cmp_operator(lex)
            if sql_op:
                expr = SQLBiOp(sql_op, expr,
                               SQLBiOp.parse_bit_or_expression(lex))
                continue

            is_op = SQLBiOp.consume_is_operator(lex)
            if is_op:
                expr = SQLUniOp(is_op, expr)
                continue

            inverted = ''
            if lex.consume('NOT'):
                inverted = 'NOT '

            if lex.consume('LIKE'):
                expr = SQLLike(inverted + 'LIKE', expr,
                               SQLBiOp.parse_bit_or_expression(lex))
            elif lex.consume('BETWEEN'):
                expr_l = SQLBiOp.parse_bit_or_expression(lex)
                lex.expect('AND')
                expr_r = SQLBiOp.parse_bit_or_expression(lex)
                expr = SQLBetween(inverted + 'BETWEEN',
                                  expr, expr_l, expr_r)
            elif lex.consume('IN'):
                lex.expect('(')
                in_query = SQLQuery.consume(lex)
                if in_query:
                    lex.expect(')')
                    return SQLINSQL(inverted + 'IN', expr, in_query)
                vals = []
                while True:
                    vals.append(SQLBiOp.parse_bit_or_expression(lex))
                    if not lex.consume(','):
                        break
                lex.expect(')')
                return SQLIN(inverted + 'IN', expr, SQLNodeList(vals))
            else:
                break

        return expr

    @staticmethod
    def parse_bit_or_expression(lex) -> SQLExpr:
        expr = SQLBiOp.parse_bit_xor_expression(lex)

        while lex.consume('|'):
            expr = SQLBiOp('|', expr, SQLBiOp.parse_bit_xor_expression(lex))

        return expr

    @staticmethod
    def parse_bit_xor_expression(lex) -> SQLExpr:
        expr = SQLBiOp.parse_bit_and_expression(lex)

        while lex.consume('^'):
            expr = SQLBiOp('^', expr, SQLBiOp.parse_bit_and_expression(lex))

        return expr

    @staticmethod
    def parse_bit_and_expression(lex):
        expr = SQLBiOp.parse_bit_shift_expression(lex)

        while lex.consume('&'):
            expr = SQLBiOp('&', expr,
                           SQLBiOp.parse_bit_shift_expression(lex))

        return expr

    @staticmethod
    def parse_bit_shift_expression(lex):
        expr = SQLBiOp.parse_add_sub_expression(lex)

        while True:
            if lex.consume('<<'):
                expr = SQLBiOp('<<', expr,
                               SQLBiOp.parse_add_sub_expression(lex))
            elif lex.consume('>>'):
                expr = SQLBiOp('>>', expr,
                               SQLBiOp.parse_add_sub_expression(lex))
            else:
                break

        return expr

    @staticmethod
    def parse_add_sub_expression(lex):
        expr = SQLBiOp.parse_mul_div_expression(lex)

        while True:
            if lex.consume('+'):
                expr = SQLBiOp('+', expr,
                               SQLBiOp.parse_mul_div_expression(lex))
            elif lex.consume('-'):
                expr = SQLBiOp('-', expr,
                               SQLBiOp.parse_mul_div_expression(lex))
            else:
                break

        return expr

    @staticmethod
    def parse_mul_div_expression(lex):
        expr = SQLBiOp.parse_str_expression(lex)

        while True:
            if lex.consume('*'):
                expr = SQLBiOp('*', expr,
                               SQLBiOp.parse_str_expression(lex))
            elif lex.consume('/'):
                expr = SQLBiOp('/', expr,
                               SQLBiOp.parse_str_expression(lex))
            else:
                break

        return expr

    @staticmethod
    def parse_str_expression(lex):
        expr = SQLUniOp.parse(lex)

        while True:
            if lex.consume('||'):
                expr = SQLBiOp('||', expr,
                               SQLUniOp.parse(lex))
            else:
                break

        return expr


# Pass in priority for expanding on the brackets.
@dataclass(frozen=True)
class SQLUniOp(SQLExpr):
    sql_op: str
    arg: SQLExpr

    def sqlf(self, compact):
        if self.sql_op.startswith('IS'):
            return LB(
                [self.arg.sqlf(compact), TB(' '), TB(self.sql_op)])

        if self.sql_op.isalpha():
            return LB(
                [TB(self.sql_op), TB(' '), self.arg.sqlf(compact)])

        return LB([TB(self.sql_op), self.arg.sqlf(compact)])

    @staticmethod
    def parse(lex):
        if lex.consume('-'):
            expr = SQLUniOp('-', SQLCase.parse(lex))
        elif lex.consume('~'):
            expr = SQLUniOp('~', SQLCase.parse(lex))
        else:
            expr = SQLCase.parse(lex)

        return expr


@dataclass(frozen=True)
class SQLBetween(SQLExpr):
    sql_op: str
    bexpr: SQLExpr
    left: SQLExpr
    right: SQLExpr

    def sqlf(self, compact):
        return LB([
            self.bexpr.sqlf(compact), TB(' '), TB(self.sql_op),
            TB(' '), self.left.sqlf(compact), TB(' '),
            TB('AND'), TB(' '), self.right.sqlf(compact)
        ])


@dataclass(frozen=True)
class SQLLike(SQLExpr):
    sql_op: str
    lexpr: SQLExpr
    match: SQLExpr

    def sqlf(self, compact):
        return LB([
            self.lexpr.sqlf(True), TB(' ' + self.sql_op + ' '),
            self.match.sqlf(True)
        ])


@dataclass(frozen=True)
class SQLINSQL(SQLExpr):
    sql_op: str
    expr: SQLExpr
    sql: SQLNode

    def sqlf(self, compact):
        sql_op = TB(' ' + self.sql_op + ' (')
        compact_sql = LB([self.expr.sqlf(True), sql_op] +
                         [self.sql.sqlf(True), TB(')')])
        if compact:
            return compact_sql

        return CB([
            compact_sql,
            SB([
                LB([self.expr.sqlf(False), sql_op]),
                IB(self.sql.sqlf(True)),
                TB(')')
            ]),
        ])


@dataclass(frozen=True)
class SQLIN(SQLExpr):
    sql_op: str
    iexpr: SQLExpr
    args: SQLNodeList[SQLExpr]

    def sqlf(self, compact):
        in_vals = []
        for arg in self.args[:-1]:
            in_vals.append(LB([arg.sqlf(True), TB(',')]))
        in_vals.append(self.args[-1].sqlf(True))
        sql_op = TB(' ' + self.sql_op + ' (')
        compact_sql = LB([self.iexpr.sqlf(True), sql_op] + in_vals +
                         [TB(')')])
        if compact:
            return compact_sql

        # TODO(scannell): Bug with separator - incorrect usage
        return CB([
            compact_sql,
            SB([
                LB([self.iexpr.sqlf(False), sql_op]),
                IB(WB(in_vals, sep=' ')),
                TB(')')
            ]),
        ])


@dataclass(frozen=True)
class SQLCase(SQLExpr):
    base_expr: Optional[SQLExpr]
    else_expr: Optional[SQLExpr]
    args: SQLNodeList

    def sqlf(self, compact):
        big_block = []
        small_block = []
        for if_then in zip(self.args[0::2], self.args[1::2]):
            if_expr = if_then[0].sqlf(False)
            if_expr_compact = if_then[0].sqlf(True)
            then_expr = if_then[1].sqlf(False)
            then_expr_compact = if_then[1].sqlf(True)

            # Most compact form - one line
            small_block.append(
                LB([
                    TB(' WHEN '), if_expr_compact, TB(' THEN '),
                    then_expr_compact
                ]))

            # Choice
            big_block.append(
                CB([
                    SB([
                        TB('WHEN'),
                        IB(if_expr),
                        TB('THEN'),
                        IB(then_expr)
                    ]),
                    SB([
                        LB([TB('WHEN '), if_expr_compact, TB(' THEN')]),
                        IB(then_expr)
                    ])
                ])
            )
        if self.else_expr:
            else_expr_compact = LB([TB(' ELSE '), self.else_expr.sqlf(True)])
            small_block.append(else_expr_compact)

            else_expr_norm = self.else_expr.sqlf(False)
            big_block.append(
                CB([
                    SB([TB('ELSE '), IB(else_expr_norm)]),
                    else_expr_compact
                ])
            )

        case_block = TB('CASE ')
        case_block_compact = TB('CASE')
        if self.base_expr:
            case_block_compact = LB([
                TB('CASE '), self.base_expr.sqlf(True)
            ])
            case_block = LB([
                TB('CASE '), self.base_expr.sqlf(False), TB(' ')
            ])

        compact_sql = LB([case_block_compact] + small_block +
                         [TB(' END')])
        if compact:
            return compact_sql

        return CB([
            compact_sql,
            SB([case_block, IB(SB(big_block)), TB('END')])
        ])

    @staticmethod
    def parse(lex):
        if not lex.consume('CASE'):
            return SQLColonCast.parse(lex)

        base_expr: Optional[SQLExpr] = None
        if not lex.peek('WHEN'):
            base_expr = SQLExpr.parse(lex)

        else_expr: Optional[SQLExpr] = None
        args = []
        while True:
            if lex.consume('WHEN'):
                args.append(SQLExpr.parse(lex))
                lex.expect('THEN')
                args.append(SQLExpr.parse(lex))
                continue
            if lex.consume('ELSE'):
                else_expr = SQLExpr.parse(lex)
                lex.expect('END')
                break
            if lex.consume('END'):
                break
            lex.error('Expected WHEN, ELSE, or END')
        return SQLCase(base_expr, else_expr, SQLNodeList(args))


@dataclass(frozen=True)
class SQLColonCast(SQLExpr):
    arg: SQLExpr
    typ: SQLType

    def sqlf(self, compact):
        return LB([
            self.arg.sqlf(True),
            TB('::'),
            self.typ.sqlf(True),
        ])

    @staticmethod
    def parse(lex) -> SQLExpr:
        expr = (SQLBrackets.consume(lex) or
                SQLBaseExpr.parse(lex))

        if lex.consume('::'):
            typ = SQLType.parse(lex)
            expr = SQLColonCast(expr, typ)

        return expr


@dataclass(frozen=True)
class SQLStruct(SQLExpr):
    exprs: SQLNodeList[SQLExpr]

    def sqlf(self, compact):
        compact_sql = LB([TB('(')] +
                         with_commas(True, self.exprs, tail=')'))
        if compact:
            return compact_sql
        return CB([
            compact_sql,
            SB([
                TB('('),
                WB(with_commas(False, self.exprs, tail=')')),
            ])
        ])


@dataclass(frozen=True)
class SQLBrackets(SQLExpr):
    query: SQLNode

    def sqlf(self, compact):
        compact_sql = LB([
            TB('('),
            self.query.sqlf(True),
            TB(')')
        ])

        if compact:
            return compact_sql

        return CB([
            compact_sql,
            SB([
                TB('('),
                IB(self.query.sqlf(compact)),
                TB(')')
            ])
        ])

    @staticmethod
    def consume(lex) -> Optional[SQLExpr]:

        if not lex.consume('('):
            return None

        # If peek of SELECT or WITH then a sub-select it is
        if lex.peek('WITH') or lex.peek('SELECT'):
            query = SQLQuery.parse(lex)
            lex.expect(')')
            return SQLBrackets(query)

        exprs = []
        while True:
            sub_expr = SQLExpr.parse(lex)
            exprs.append(sub_expr)
            if not lex.consume(','):
                break

        lex.expect(')')

        if len(exprs) == 1:
            return SQLBrackets(exprs[0])

        return SQLStruct(SQLNodeList(exprs))
