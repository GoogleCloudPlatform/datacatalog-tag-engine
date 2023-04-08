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

from . import parse


def mock_type_parser(lex):
    lex.expect('TYPE')
    return SQLConstant('TYPE')


def mock_parse_expression(lex):
    return SQLConstant(lex.parse_identifier())


class TestParser(unittest.TestCase):

    def assert_sql(self, val, fsql):
        expr = parse(val)
        self.assertEqual(str(expr), fsql)

        nsql = expr.as_sql(False)
        nexpr = parse(nsql)
        self.assertEqual(str(expr), str(nexpr))

    def test_basic_sql(self):
        self.assert_sql('/* x */ select E1 as x, E2, E3 as i',
                        'SELECT E1 AS x,E2,E3 AS i;')

    def test_select(self):
        self.assert_sql('/* x */ select EXPR as x, EXPR, EXPR as i',
                        'SELECT EXPR AS x,EXPR,EXPR AS i;')
        self.assert_sql('select EXPR as fieldA where EXPR group by fieldA',
                        'SELECT EXPR AS fieldA WHERE EXPR GROUP BY fieldA;')
        self.assert_sql('/* x */ select EXPR as x, EXPR, EXPR as i',
                        'SELECT EXPR AS x,EXPR,EXPR AS i;')
        self.assert_sql('SELECT EXPR UNION ALL SELECT EXPR',
                        'SELECT EXPR UNION ALL SELECT EXPR;')

    def test_withselect(self):
        self.assert_sql('with x as (select E1) select E2 as fieldA '
                        'where WEXPR group by fieldA;',
                        'WITH x AS(SELECT E1)SELECT E2 AS fieldA '
                        'WHERE WEXPR GROUP BY fieldA;')

    # TODO(scannell): Generate SQL for user defined functions as well!
    @mock.patch('sql_parser.types.SQLType.parse',
                mock.MagicMock(side_effect=mock_type_parser))
    def test_function(self):
        self.assert_sql('CREATE TEMPORARY FUNCTION TO_STRING(x TYPE) AS'
                        ' FExpr; SELECT F;',
                        'CREATE TEMPORARY FUNCTION TO_STRING(x TYPE)'
                        'AS FExpr;SELECT F;')
        self.assert_sql('CREATE TEMPORARY FUNCTION stuff(x TYPE) '
                        'RETURNS TYPE '
                        'LANGUAGE js AS """ javascript stuff """;'
                        'SELECT F;',
                        'CREATE TEMPORARY FUNCTION stuff(x TYPE)'
                        'RETURNS TYPE LANGUAGE '
                        'js AS \' javascript stuff \';SELECT F;')

    def test_order_by(self):
        self.assert_sql('SELECT x FROM t ORDER BY tx,tb ASC',
                        'SELECT x FROM t ORDER BY tx,tb ASC;')
        self.assert_sql('SELECT x FROM t ORDER BY tx DESC,tb ASC',
                        'SELECT x FROM t ORDER BY tx DESC,tb ASC;')

    def test_limit_offset(self):
        self.assert_sql('SELECT x FROM t LIMIT 10', 'SELECT x FROM t LIMIT 10;')
        self.assert_sql('SELECT x FROM t LIMIT 10 OFFSET 10',
                        'SELECT x FROM t LIMIT 10 OFFSET 10;')

    def test_query_table(self):
        self.assert_sql('SELECT E1 FROM(SELECT stuff FROM t)f',
                        'SELECT E1 FROM(SELECT stuff FROM t)AS f;')

    def test_sub_query(self):
        self.assert_sql('(SELECT F1 FROM stuff)ORDER BY F2',
                        '(SELECT F1 FROM stuff)ORDER BY F2;')
        self.assert_sql('(SELECT F1 FROM stuff ORDER BY f)ORDER BY F2',
                        '(SELECT F1 FROM stuff ORDER BY f)ORDER BY F2;')
        self.assert_sql('select F1 FROM stuff ORDER BY F2',
                        'SELECT F1 FROM stuff ORDER BY F2;')

    def test_having(self):
        self.assert_sql('SELECT Fx FROM t GROUP BY Tx HAVING E1',
                        'SELECT Fx FROM t GROUP BY Tx HAVING E1;')

    def test_select_type(self):
        self.assert_sql('SELECT DISTINCT f FROM t', 'SELECT DISTINCT f FROM t;')
        self.assert_sql('SELECT ALL f FROM t', 'SELECT ALL f FROM t;')

    def test_sets(self):
        self.assert_sql('SELECT x FROM t UNION ALL SELECT x FROM t',
                        'SELECT x FROM t UNION ALL SELECT x FROM t;')
        self.assert_sql('SELECT x FROM t UNION DISTINCT SELECT x FROM t',
                        'SELECT x FROM t UNION DISTINCT SELECT x FROM t;')
        self.assert_sql('SELECT x FROM t INTERSECT DISTINCT SELECT x FROM t',
                        'SELECT x FROM t INTERSECT DISTINCT SELECT x FROM t;')
        self.assert_sql('SELECT x FROM t EXCEPT DISTINCT SELECT x FROM t',
                        'SELECT x FROM t EXCEPT DISTINCT SELECT x FROM t;')

    # TODO(scannell)
    # Sub-query as table
    def test_select_expr(self):
        self.assert_sql('SELECT X FROM Y WHERE F IN (SELECT A FROM B)',
                        'SELECT X FROM Y WHERE F IN(SELECT A FROM B);')
        self.assert_sql('SELECT (SELECT 1)',
                        'SELECT(SELECT 1);')

    def test_join(self):
        self.assert_sql('SELECT X FROM Table Y JOIN Table X ON Expr1',
                        'SELECT X FROM Table AS Y INNER JOIN Table AS X '
                        'ON Expr1;')
        self.assert_sql('SELECT X FROM Table Y LEFT JOIN Table X',
                        'SELECT X FROM Table AS Y LEFT JOIN Table AS X;')
        self.assert_sql('SELECT X FROM Table Y INNER JOIN Table X',
                        'SELECT X FROM Table AS Y INNER JOIN Table AS X;')
        self.assert_sql('SELECT X FROM Table Y CROSS JOIN Table X',
                        'SELECT X FROM Table AS Y CROSS JOIN Table AS X;')
        self.assert_sql('SELECT X FROM Table Y, Table X',
                        'SELECT X FROM Table AS Y CROSS JOIN Table AS X;')
        self.assert_sql('SELECT X FROM Table Y LEFT OUTER JOIN Table X',
                        'SELECT X FROM Table AS Y LEFT OUTER JOIN Table AS X;')
        self.assert_sql('SELECT X FROM Table Y RIGHT OUTER JOIN Table X',
                        'SELECT X FROM Table AS Y RIGHT OUTER JOIN Table AS X;')
        self.assert_sql('SELECT X FROM Table Y RIGHT JOIN Table X',
                        'SELECT X FROM Table AS Y RIGHT JOIN Table AS X;')
        self.assert_sql('SELECT X FROM Table Y FULL OUTER JOIN Table X',
                        'SELECT X FROM Table AS Y FULL OUTER JOIN Table AS X;')
        self.assert_sql('SELECT X FROM Table Y FULL JOIN Table X',
                        'SELECT X FROM Table AS Y FULL JOIN Table AS X;')

    def test_truncate(self):
        self.assert_sql('TRUNCATE  TABLE /* b */ tt;',
                        'TRUNCATE TABLE tt;')
        self.assert_sql('truncate table td_address',
                        'TRUNCATE TABLE td_address;')

    def test_multiple(self):
        self.assert_sql('TRUNCATE  TABLE /* b */ tt;  SELECT  1;',
                        'TRUNCATE TABLE tt;SELECT 1;')

    def test_insert(self):
        self.assert_sql('INSERT INTO TABLE (a,b,c) SELECT 1,2;',
                        'INSERT INTO TABLE(a,b,c)SELECT 1,2;')
        self.assert_sql('INSERT INTO TABLE (SELECT 1,2);',
                        'INSERT INTO TABLE(SELECT 1,2);')

    def test_generate_stats(self):
        self.assert_sql('GENERATE   STATistics on blah;',
                        'GENERATE STATISTICS ON blah;')
        self.assert_sql('GROOM TABLE x RECORDS ALL;',
                        'GROOM TABLE x RECORDS ALL;')

    def test_update(self):
        self.assert_sql('UPDATE x SET f1=1, f2=a+b FROM y WHERE TRUE',
                        'UPDATE x SET f1=1,f2=a+b FROM y WHERE TRUE;')

    def test_trans(self):
        self.assert_sql('BEGIN;', 'BEGIN;')

    def test_sql_expr(self):
        self.assert_sql('select ((select 1) + 2)',
                        'SELECT((SELECT 1)+2);')

    def test_sql_delete(self):
        self.assert_sql('DELETE from y where true',
                        'DELETE FROM y WHERE TRUE;')

    def test_sql_create(self):
        self.assert_sql('CREATE OR replace TABLE y (i INT64, b BOOL) AS SELECT 1',
                        'CREATE OR REPLACE TABLE y(i INT64,b BOOL)AS SELECT 1;')
