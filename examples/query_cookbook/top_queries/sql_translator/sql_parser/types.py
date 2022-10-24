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

"""SQL Types.

Parsing and AST Nodes
"""

from dataclasses import dataclass
from typing import Optional
from typing import List

from sql_translator.rfmt.blocks import LineBlock as LB
from sql_translator.rfmt.blocks import TextBlock as TB
from sql_translator.rfmt.blocks import WrapBlock as WB

from sql_translator.sql_parser.node import SQLNode
from sql_translator.sql_parser.node import SQLNodeList

from sql_translator.sql_parser.const import SQLNumber
from sql_translator.sql_parser.ident import SQLIdentifier


@dataclass(frozen=True)
class SQLType(SQLNode):

    @staticmethod
    def parse(lex) -> 'SQLType':
        return (SQLType.consume(lex) or
                lex.error('expected type'))

    @staticmethod
    def consume(lex) -> 'Optional[SQLType]':
        return (SQLConcreteType.consume(lex) or
                SQLVarchar.consume(lex) or
                SQLArray.consume(lex) or
                SQLStruct.consume(lex))


@dataclass(frozen=True)
class SQLConcreteType(SQLType):
    """Concrete SQLConcreteType.
    """
    value: str

    def sqlf(self, compact):
        del compact  # Unused
        return TB(self.value)

    @staticmethod
    def consume(lex) -> 'Optional[SQLConcreteType]':
        typ = lex.consume_any(['INT64', 'FLOAT64', 'BOOL', 'BYTES', 'DATE',
                               'DATETIME', 'TIME', 'TIMESTAMP', 'STRING',
                               'BIGINT', 'SMALLINT', 'INT4', 'ABSTIME',
                               'INTEGER'])
        if not typ:
            return None

        return SQLConcreteType(typ)


@dataclass(frozen=True)
class SQLNamedType(SQLType):
    """Named SQL Type.
    """
    tname: SQLIdentifier
    type: Optional[SQLType]

    def sqlf(self, compact):
        return LB([
            self.tname.sqlf(compact),
            TB(' '),
            self.type.sqlf(compact)
        ])


@dataclass(frozen=True)
class SQLArray(SQLType):
    """SQL Array.
    """
    atype: SQLType

    def sqlf(self, compact):
        return LB([
            TB('ARRAY<'),
            self.atype.sqlf(compact),
            TB('>')
        ])

    @staticmethod
    def consume(lex) -> 'Optional[SQLArray]':
        if not lex.consume('ARRAY'):
            return None

        lex.expect('<')
        ntype = SQLType.parse(lex)
        lex.expect('>')

        return SQLArray(ntype)


@dataclass(frozen=True)
class SQLVarchar(SQLType):
    size: SQLNode

    def sqlf(self, compact):
        return LB([
            TB('VARCHAR('),
            self.size.sqlf(True),
            TB(')')
        ])

    @staticmethod
    def consume(lex) -> 'Optional[SQLVarchar]':
        if not lex.consume('VARCHAR'):
            return None

        lex.expect('(')
        size = (SQLNumber.consume(lex) or
                lex.error('expected number'))
        lex.expect(')')

        return SQLVarchar(size)


@dataclass(frozen=True)
class SQLStruct(SQLType):
    """SQL Structure.
    """
    vals: SQLNodeList[SQLType]

    def sqlf(self, compact):
        sep = ','
        if compact:
            sep += ' '

        fields = []
        for field in self.vals[0:-1]:
            fields.append(LB([
                field.sqlf(True), TB(sep)
            ]))
        fields.append(self.vals[-1].sqlf(True))

        if compact:
            return LB(
                [TB('STRUCT<')] +
                fields +
                [TB('>')]
            )
        return LB([
            TB('STRUCT<'),
            WB(fields),
            TB('>')
        ])

    @staticmethod
    def consume(lex) -> 'Optional[SQLStruct]':
        if not lex.consume('STRUCT'):
            return None

        lex.expect('<')
        types: List[SQLType] = []
        while True:
            ptype = SQLType.consume(lex)
            if ptype is None:
                name = SQLIdentifier.parse(lex)
                types.append(SQLNamedType(name, SQLType.parse(lex)))
            else:
                types.append(ptype)
            if not lex.consume(','):
                break
        lex.expect('>')
        return SQLStruct(SQLNodeList(types))
