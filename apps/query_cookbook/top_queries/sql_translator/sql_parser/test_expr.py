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

import unittest
import mock

from .const import SQLConstant
from .lexer import ParsingError, SQLLexer
from .expr import SQLExpr


def mock_type_parser(lex):
    lex.expect('TYPE')
    return SQLConstant('TYPE')


@mock.patch('sql_parser.expr_funcs.SQLType.parse',
            mock.MagicMock(side_effect=mock_type_parser))
@mock.patch('sql_parser.expr_op.SQLType.parse',
            mock.MagicMock(side_effect=mock_type_parser))
class TestExpr(unittest.TestCase):

    def exprt(self, val, check):
        # Check parsed tree against expected tree
        expr = SQLExpr.parse(SQLLexer(val))
        self.assertEqual(str(expr), check)

        # Check re-parsed generated SQL against expected tree
        val2 = expr.as_sql(True)
        expr2 = SQLExpr.parse(SQLLexer(val2))
        self.assertEqual(str(expr2), check)

        # Check non-compact re-parsed generated SQL against expected tree
        val3 = expr.as_sql(False)
        expr3 = SQLExpr.parse(SQLLexer(val3))
        self.assertEqual(str(expr3), check)

    def test_bitoppexprsql(self):
        self.exprt('i>>j', 'i>>j')
        self.exprt('i<<j', 'i<<j')
        self.exprt('i>j', 'i>j')
        self.exprt('i<j', 'i<j')
        self.exprt('i>=j', 'i>=j')
        self.exprt('i<=j', 'i<=j')
        self.exprt('i!=j', 'i!=j')
        self.exprt('i^j', 'i^j')
        self.exprt('i|j', 'i|j')
        self.exprt('i&j', 'i&j')

    def test_biopexprsql(self):
        # Test with tokenization
        self.exprt('i+j+k', 'i+j+k')
        self.exprt('i*j+k', 'i*j+k')
        self.exprt('i+j*k', 'i+j*k')
        self.exprt('i+(j/k)', 'i+(j/k)')
        self.exprt('(i+(j*k))', '(i+(j*k))')
        self.exprt('i*(j+k)', 'i*(j+k)')
        self.exprt('i+(j*l+k)', 'i+(j*l+k)')

    def test_biopexpr(self):
        self.exprt('f >> g', 'f>>g')
        self.exprt('f + g', 'f+g')
        self.exprt('f- -g', 'f- -g')
        self.exprt('f- ~g', 'f-~g')
        self.exprt('(a+b)*c', '(a+b)*c')
        self.exprt('f-g', 'f-g')
        self.exprt('F(a)-b', 'F(a)-b')
        self.exprt('F(a)- -b', 'F(a)- -b')

    def test_logicexpr(self):
        self.exprt('A and B', 'A AND B')
        self.exprt('not X and Y',
                   'NOT X AND Y')
        self.exprt('A or B and C',
                   'A OR B AND C')
        self.exprt('A and B or C',
                   'A AND B OR C')

    def test_is(self):
        self.exprt('A is null', 'A IS NULL')
        self.exprt('A is not null', 'A IS NOT NULL')
        self.exprt('A is true', 'A IS TRUE')
        self.exprt('A is not true', 'A IS NOT TRUE')
        self.exprt('A is false', 'A IS FALSE')
        self.exprt('A is not False', 'A IS NOT FALSE')
        self.exprt('A is null', 'A IS NULL')
        self.exprt('A is   not  /* xyz */ null',
                   'A IS NOT NULL')
        self.exprt('A like myStr', 'A LIKE myStr')
        self.exprt('A not like myStr',
                   'A NOT LIKE myStr')
        self.exprt('a between X and Y',
                   'a BETWEEN X AND Y')
        self.exprt('a NOT between X and Y',
                   'a NOT BETWEEN X AND Y')
        self.exprt('a IN (x,y,z)', 'a IN(x,y,z)')
        self.exprt('a NOT IN (x,y,z)', 'a NOT IN(x,y,z)')

        with self.assertRaises(ParsingError):
            SQLExpr.parse(SQLLexer('a is blah'))

    def test_func(self):
        self.exprt('funcA(a,b)', 'funcA(a,b)')
        self.exprt('funcA(x - i) ', 'funcA(x-i)')

    def test_special(self):
        # Date
        self.exprt('date_Add(myDateVar, INTERVAL a DAY)',
                   'DATE_ADD(myDateVar,INTERVAL a DAY)')
        self.exprt('date_sub(myDateVar, INTERVAL b QUARTER)',
                   'DATE_SUB(myDateVar,INTERVAL b QUARTER)')

        # Cast
        self.exprt('cast(myVar AS TYPE)',
                   'CAST(myVar AS TYPE)')
        self.exprt('safe_cast(yourVar AS TYPE)',
                   'SAFE_CAST(yourVar AS TYPE)')

        # Colon Cast
        self.exprt('myVar::type', 'myVar::TYPE')

        # CASE
        self.exprt('case when a then b end',
                   'CASE WHEN a THEN b END')
        self.exprt('case when x then y end',
                   'CASE WHEN x THEN y END')
        self.exprt('case when a.b=f then p end',
                   'CASE WHEN a.b=f THEN p END')
        self.exprt('case when i then j else k end',
                   'CASE WHEN i THEN j ELSE k END')
        self.exprt('case F WHEN G THEN H end',
                   'CASE F WHEN G THEN H END')

        # Extract
        self.exprt('extract(minute from MyDate)',
                   'EXTRACT(minute FROM MyDate)')

        # Count
        self.exprt('count(a)', 'COUNT(a)')
        self.exprt('count(distinct a)', 'COUNT(DISTINCT a)')

        # Interval
        self.exprt('INTERVAL \'1 second\'',
                   'INTERVAL(\'1 second\')')
        self.exprt('INTERVAL(\'1 second\')',
                   'INTERVAL(\'1 second\')')

        # Array AGG
        # TODO(scannell)

    def test_in(self):
        self.exprt('a IN (SELECT 1)', 'a IN(SELECT 1)')
        self.exprt('(a,b) IN (SELECT 1)', '(a,b)IN(SELECT 1)')

    def test_analytic_navigation(self):
        self.exprt('last_value(a ignore nulls)',
                   'LAST_VALUE(a IGNORE NULLS)')

    def test_partition_over(self):
        self.exprt('last_value(x ignore nulls) over ()',
                   'LAST_VALUE(x IGNORE NULLS)OVER()')

    def test_exists(self):
        self.exprt('exists(select 1)',
                   'EXISTS(SELECT 1)')

    def test_array_agg(self):
        self.exprt('ARRAY_AGG(DISTINCT   x  LIMIT  1)',
                   'ARRAY_AGG(DISTINCT x LIMIT 1)')

    def test_array_lit(self):
        self.exprt('[1,2,3,4]', '[1,2,3,4]')

    def test_struct(self):
        self.exprt('("a", 1)', '(\'a\',1)')

    def test_array_select(self):
        self.exprt('ARRAY(SELECT x FROM abc)',
                   'ARRAY(SELECT x FROM abc)')
