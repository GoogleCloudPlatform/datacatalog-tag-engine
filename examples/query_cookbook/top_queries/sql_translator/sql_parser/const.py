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

"""SQL Lexer - parse basic constructs from a StringReader.

"""

from dataclasses import dataclass
from typing import Any
from typing import Optional
from typing import Union

from sql_translator.rfmt.blocks import TextBlock as TB

from sql_translator.sql_parser.expr import SQLExpr


@dataclass(frozen=True)
class SQLConstant(SQLExpr):
    value: Any = None
    """SQLConstant - basic types.

    Attributes:
      val: Python value type
    """

    def sqlf(self, compact):
        del compact  # Unused
        return TB(str(self.value))

    @staticmethod
    def consume(lex) -> 'Optional[SQLConstant]':
        return (SQLNull.consume(lex) or
                SQLBool.consume(lex) or
                SQLNumber.consume(lex) or
                SQLString.consume(lex))


@dataclass(frozen=True)
class SQLNull(SQLConstant):
    """SQLNull - NULL constant.
    """
    def sqlf(self, compact):
        del compact  # Unused
        return TB('NULL')

    @staticmethod
    def consume(lex) -> 'Optional[SQLNull]':
        if lex.consume('NULL'):
            return SQLNull()
        return None


@dataclass(frozen=True)
class SQLNumber(SQLConstant):
    value: Union[float, int]

    @staticmethod
    def consume(lex) -> 'Optional[SQLNumber]':
        num = lex.consume_number()
        if not num:
            return None

        return SQLNumber(num)


@dataclass(frozen=True)
class SQLBool(SQLConstant):
    value: bool
    """SQLBool - basic types.

    Attributes:
      val: Python true/false
    """
    def sqlf(self, compact):
        del compact  # Unused
        return TB(str(self.value).upper())

    @staticmethod
    def consume(lex) -> 'Optional[SQLBool]':
        if lex.consume('TRUE'):
            return SQLBool(True)
        if lex.consume('FALSE'):
            return SQLBool(False)
        return None


@dataclass(frozen=True)
class SQLString(SQLConstant):
    value: str
    flag: Optional[str] = None
    """SQLString - basic string.

    Attributes:
      val: Python string
    """

    ESCAPE_CHARS = ["\\", "'"]

    def sqlf(self, compact):
        del compact  # Unused

        # Escape characters
        out_string = self.value
        for escape_char in SQLString.ESCAPE_CHARS:
            out_string = out_string.replace(escape_char, "\\" + escape_char)

        return TB('{}\'{}\''.format(
            self.flag or '',
            out_string,
        ))

    @staticmethod
    def consume(lex) -> 'Optional[SQLString]':
        sql_str = lex.consume_string()
        if not sql_str:
            return None

        return SQLString(sql_str[0], sql_str[1])
