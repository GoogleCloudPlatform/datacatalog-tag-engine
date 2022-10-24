# Copyright 2020 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Parser generator for SQL Commands independent data operations.

  Usage:
    cmd = SQLCommand.consume(lex)

"""

from dataclasses import dataclass
from typing import Optional

from sql_translator.rfmt.blocks import LineBlock as LB
from sql_translator.rfmt.blocks import TextBlock as TB

from sql_translator.sql_parser.query import SQLNamedTable
from sql_translator.sql_parser.node import SQLNode


@dataclass(frozen=True)
class SQLCommand(SQLNode):

    @staticmethod
    def consume(lex) -> 'Optional[SQLCommand]':
        return (SQLTruncate.consume(lex) or
                SQLGenerateStatistics.consume(lex) or
                SQLGroom.consume(lex) or
                SQLTrans.consume(lex))


@dataclass(frozen=True)
class SQLTruncate(SQLCommand):
    table: SQLNamedTable

    def sqlf(self, compact):
        return LB([
            TB('TRUNCATE TABLE '),
            self.table.sqlf(compact=True)
        ])

    @staticmethod
    def consume(lex) -> 'Optional[SQLTruncate]':
        if not lex.consume('TRUNCATE'):
            return None

        lex.consume('TABLE')

        table_name = SQLNamedTable.parse(lex, is_write=True)

        return SQLTruncate(table_name)


@dataclass(frozen=True)
class SQLGenerateStatistics(SQLCommand):
    table: SQLNamedTable

    def sqlf(self, compact):
        return LB([
            TB('GENERATE STATISTICS ON '),
            self.table.sqlf(compact=True)
        ])

    @staticmethod
    def consume(lex) -> 'Optional[SQLGenerateStatistics]':
        if not lex.consume(['GENERATE', 'STATISTICS', 'ON']):
            return None

        lex.consume('TABLE')
        table = SQLNamedTable.parse(lex, is_write=True)

        return SQLGenerateStatistics(table)


@dataclass(frozen=True)
class SQLGroom(SQLCommand):
    table: SQLNamedTable

    def sqlf(self, compact):
        return LB([
            TB('GROOM TABLE '),
            self.table.sqlf(compact=True),
            TB(' RECORDS ALL'),
        ])

    @staticmethod
    def consume(lex) -> 'Optional[SQLGroom]':
        if not lex.consume(['GROOM', 'TABLE']):
            return None

        table = SQLNamedTable.parse(lex, is_write=True)

        lex.expect('RECORDS')
        lex.expect('ALL')

        return SQLGroom(table)


@dataclass(frozen=True)
class SQLTrans(SQLCommand):
    trans: str

    def sqlf(self, compact):
        return TB(self.trans)

    @staticmethod
    def consume(lex) -> 'Optional[SQLTrans]':
        if lex.consume('BEGIN'):
            return SQLTrans('BEGIN')
        if lex.consume('COMMIT'):
            return SQLTrans('COMMIT')
        if lex.consume('ABORT'):
            return SQLTrans('ABORT')
        return None
