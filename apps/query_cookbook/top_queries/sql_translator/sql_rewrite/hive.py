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


from sql_translator.sql_parser.expr_op import SQLBiOp
from sql_translator.sql_parser.expr_op import SQLUniOp
from sql_translator.sql_parser.expr_op import SQLBrackets
from sql_translator.sql_parser.expr_base import SQLFuncExpr
from sql_translator.sql_parser.const import SQLBool
from sql_translator.sql_parser.node import SQLNodeList

from sql_translator.sql_rewrite.rewrite import rewrite_expr
from sql_translator.sql_rewrite.bigquery_cleanup import cleanup


# Do conversion
def convert(expr):
    expr = rewrite_expr(expr, null_equal_to_coalesce)
    expr = cleanup(expr)
    return expr


# Rewrite null equal
def null_equal_to_coalesce(expr):
    if not isinstance(expr, SQLBiOp):
        return None

    if expr.sql_op != '<=>':
        return None

    return SQLFuncExpr('COALESCE', SQLNodeList([
        SQLBrackets(SQLBiOp('=', expr.left, expr.right)),
        SQLBrackets(SQLBiOp('AND',
                            SQLUniOp('IS NULL', expr.left),
                            SQLUniOp('IS NULL', expr.right))),
        SQLBool(False)
    ]))
