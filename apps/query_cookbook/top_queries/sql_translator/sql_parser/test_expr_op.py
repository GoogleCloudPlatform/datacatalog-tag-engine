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


def mock_parse_base(lex):
    return SQLConstant(lex.consume_identifier())


@mock.patch('sql_parser.expr.SQLBaseExpr.parse',
            mock.MagicMock(side_effect=mock_parse_base))
class TestExprOp(unittest.TestCase):

    def exprt(self, v, c, r):
        # Check parsed tree against expected tree
        expr = SQLExpr.parse(SQLLexer(v))
        self.assertEqual(str(expr), c)

        # Check re-parsed generated SQL against expected tree
        v2 = expr.as_sql(True)
        expr2 = SQLExpr.parse(SQLLexer(v2))
        self.assertEqual(str(expr2), c)

        # Check non-compact re-parsed generated SQL against expected tree
        v2 = expr.as_sql(False)
        expr2 = SQLExpr.parse(SQLLexer(v2))

        # Check the representative format (get_tree)
        # TODO(scannell) bring this back!
        # self.assertEqual(parse_expression(SQLLexer(v)).get_repr(), r)

    @mock.patch('sql_parser.expr.SQLBaseExpr.parse',
                mock.MagicMock(side_effect=mock_parse_base))
    def test_bitoppexprsql(self):
        self.exprt('i>>j', 'i>>j', '>>(i,j)')
        self.exprt('i<<j', 'i<<j', '<<(i,j)')
        self.exprt('i>j', 'i>j', '>(i,j)')
        self.exprt('i<j', 'i<j', '<(i,j)')
        self.exprt('i>=j', 'i>=j', '>=(i,j)')
        self.exprt('i<=j', 'i<=j', '<=(i,j)')
        self.exprt('i!=j', 'i!=j', '!=(i,j)')
        self.exprt('i^j', 'i^j', '^(i,j)')
        self.exprt('i|j', 'i|j', '|(i,j)')
        self.exprt('i&j', 'i&j', '&(i,j)')

    def test_biopexprsql(self):
        # Test with tokenization
        self.exprt('i+j+k', 'i+j+k', '+(+(i,j),k)')
        self.exprt('i*j+k', 'i*j+k', '+(*(i,j),k)')
        self.exprt('i+j*k', 'i+j*k', '+(i,*(j,k))')

    def test_biopexpr(self):
        self.exprt('f >> g', 'f>>g', '>>(f,g)')
        self.exprt('f + g', 'f+g', '+(f,g)')
        self.exprt('f- -g', 'f- -g', '-(f,-(g))')
        self.exprt('f- ~g', 'f-~g', '-(f,~(g))')
        self.exprt('f-g', 'f-g', '-(f,g)')

    def test_logicexpr(self):
        self.exprt('A and B', 'A AND B', 'AND(A,B)')
        self.exprt('not X and Y',
                   'NOT X AND Y',
                   'AND(NOT(X),Y)')
        self.exprt('A or B and C',
                   'A OR B AND C',
                   'OR(A,AND(B,C))')
        self.exprt('A and B or C',
                   'A AND B OR C',
                   'OR(AND(A,B),C)')
    
    def test_is(self):
        self.exprt('A is null', 'A IS NULL', 'IS NULL(A)')
        self.exprt('A is not null', 'A IS NOT NULL', 'IS NOT NULL(A)')
        self.exprt('A is true', 'A IS TRUE', 'IS TRUE(A)')
        self.exprt('A is not true', 'A IS NOT TRUE', 'IS NOT TRUE(A)')
        self.exprt('A is false', 'A IS FALSE', 'IS FALSE(A)')
        self.exprt('A is not False', 'A IS NOT FALSE', 'IS NOT FALSE(A)')
        self.exprt('A is null', 'A IS NULL', 'IS NULL(A)')
        self.exprt('A is   not  /* xyz */ null',
                   'A IS NOT NULL',
                   'IS NOT NULL(A)')
        self.exprt('A like myStr', 'A LIKE myStr', 'LIKE(A,myStr)')
        self.exprt('A not like myStr',
                   'A NOT LIKE myStr',
                   'NOT LIKE(A,myStr)')
        self.exprt('a between X and Y',
                   'a BETWEEN X AND Y',
                   'BETWEEN(a,X,Y)')
        self.exprt('a NOT between X and Y',
                   'a NOT BETWEEN X AND Y',
                   'NOT BETWEEN(a,X,Y)')
        self.exprt('a IN (x,y,z)', 'a IN(x,y,z)', 'IN(a,x,y,z)')
        self.exprt('a NOT IN (x,y,z)', 'a NOT IN(x,y,z)', 'NOT IN(a,x,y,z)')

        with self.assertRaises(ParsingError):
            SQLExpr.parse(SQLLexer('a is blah'))
