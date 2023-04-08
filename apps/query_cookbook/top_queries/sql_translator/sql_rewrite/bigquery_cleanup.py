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


from sql_translator.sql_parser.expr_op import (SQLCase, SQLBiOp)
from sql_translator.sql_parser.expr_base import SQLFuncExpr
from sql_translator.sql_parser.ident import SQLIdentifier

from sql_translator.sql_parser.node import SQLNodeList

from sql_translator.sql_rewrite.rewrite import rewrite_expr

# Do rewrite
def cleanup(expr):
    expr = rewrite_expr(expr, convert_case_to_if)
    return expr


# Convert CASE statements into IF()
def convert_case_to_if(expr):
    if not isinstance(expr, SQLCase):
        return None

    # There must be an ELSE condition and only one CASE
    if len(expr.args) != 2 or not expr.else_expr:
        return None

    # Base condition
    if_cond = expr.args[0]
    if expr.base_expr:
        if_cond = SQLBiOp('=', expr.base_expr, if_cond)

    # Convert to IF
    return SQLFuncExpr([SQLIdentifier('IF')], SQLNodeList([
        if_cond,
        expr.args[1],
        expr.else_expr,
    ]))
