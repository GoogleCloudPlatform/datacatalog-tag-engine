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


from sql_translator.sql_parser.parser import (SQLScript)
from sql_translator.sql_parser.cmd import (SQLTruncate, SQLTrans, SQLGroom,
                            SQLGenerateStatistics)
from sql_translator.sql_parser.dml import (SQLDelete)
from sql_translator.sql_parser.expr_op import (SQLColonCast, SQLCase)
from sql_translator.sql_parser.expr_funcs import SQLCAST
from sql_translator.sql_parser.expr_base import SQLFuncExpr

from sql_translator.sql_parser.types import SQLConcreteType, SQLVarchar
from sql_translator.sql_parser.node import SQLNodeList

from sql_translator.sql_rewrite.rewrite import rewrite_expr
from sql_translator.sql_rewrite.bigquery_cleanup import cleanup


# Do conversion
def convert(expr):
    expr = rewrite_expr(expr, remove_unsupported)
    expr = rewrite_expr(expr, function_map)
    expr = rewrite_expr(expr, convert_decode)
    expr = rewrite_expr(expr, colon_cast_to_safecast)
    expr = rewrite_expr(expr, type_convert)
    expr = cleanup(expr)
    return expr


# Colon cast fix
def colon_cast_to_safecast(expr):
    if not isinstance(expr, SQLColonCast):
        return None

    return SQLCAST('SAFE_CAST', expr.arg, expr.typ)


# Type mapping
primitive_type_map = {
    'BIGINT': 'INT64',
    'SMALLINT': 'INT64',
    'INT4': 'INT64',
    'ABSTIME': 'TIMESTAMP',
    'INTEGER': 'INT64',
}


def type_convert(expr):
    if isinstance(expr, SQLVarchar):
        return SQLConcreteType('STRING')

    if not isinstance(expr, SQLConcreteType):
        return None

    nvalue = primitive_type_map.get(expr.value, None)
    if nvalue is None:
        return None

    return SQLConcreteType(nvalue)


# Truncate table
def convert_truncate(expr):
    if not isinstance(expr, SQLTruncate):
        return None

    return SQLDelete(expr.table, None, SQLConcreteType('TRUE'))


# Remove assorted things
def remove_unsupported(expr):
    if not isinstance(expr, SQLScript):
        return None

    ncmds = []
    for cmd in expr.commands:
        if isinstance(cmd, SQLTrans):
            continue
        if isinstance(cmd, SQLGroom):
            continue
        if isinstance(cmd, SQLGenerateStatistics):
            continue
        ncmds.append(cmd)

    return SQLScript(SQLNodeList(ncmds))


# Simple function maps
def function_map(expr):
    if not isinstance(expr, SQLFuncExpr):
        return None

    if expr.name == 'INSTR':
        return SQLFuncExpr('STRPOS', expr.args)

    return None


# Convert decode
# (Be wary of NULL values - different from case)
def convert_decode(expr):
    if not isinstance(expr, SQLFuncExpr):
        return None

    if expr.name.upper() != 'DECODE':
        return None

    # It translates directly, except for the oddity around NULLs.
    return SQLCase(SQLNodeList(expr.args))
