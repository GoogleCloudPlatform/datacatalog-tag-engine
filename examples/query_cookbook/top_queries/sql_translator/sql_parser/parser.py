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


from typing import List
from typing import Optional
from dataclasses import dataclass

from sql_translator.rfmt.blocks import StackBlock as SB
from sql_translator.rfmt.blocks import LineBlock as LB
from sql_translator.rfmt.blocks import TextBlock as TB

from sql_translator.sql_parser.node import SQLNodeList
from sql_translator.sql_parser.node import SQLNode

from sql_translator.sql_parser.func import SQLFunction
from sql_translator.sql_parser.query import SQLQuery
from sql_translator.sql_parser.dml import SQLDML
from sql_translator.sql_parser.cmd import SQLCommand


@dataclass(frozen=True)
class SQLScript(SQLNode):
    commands: 'SQLNodeList[SQLScript]'

    def sqlf(self, compact):
        return SB([LB([cmd.sqlf(compact),TB(';\n')]) for cmd in self.commands])

    @staticmethod
    def parse(lex) -> 'SQLScript':
        commands: List[SQLNode] = []
        while True:
            functions: List[SQLFunction] = []
            while True:
                func = SQLFunction.consume(lex)
                if not func:
                    break
                functions.append(func)

                lex.expect(';')

            lex.consume(';')

            # Consume noise
            while lex.consume('\\time') or lex.consume('\\t'):
                continue

            main_query = (SQLQuery.consume(lex) or
                          SQLDML.consume(lex) or
                          SQLCommand.consume(lex))

            if not main_query:
                break

            if functions:
                commands.append(SQLWithFunctions(main_query,
                                                 SQLNodeList(functions)))

            else:
                commands.append(main_query)

        if not commands:
            lex.error('Expect at least one SQL statement')

        lex.expect_end()

        return SQLScript(SQLNodeList(commands))


@dataclass(frozen=True)
class SQLWithFunctions(SQLQuery):
    sql: SQLNode
    funcs: Optional[SQLNode]

    def sqlf(self, compact):
        stack = []
        if self.funcs:
            for func in self.funcs:
                stack.append(func.sqlf(compact))
        stack.append(self.sql.sqlf(compact))
        return SB(stack)

    @staticmethod
    def consume(lex):
        functions: List[SQLFunction] = []

        while True:
            func = SQLFunction.consume(lex)
            if not func:
                break
            functions.append(func)

            lex.expect(';')

        lex.consume(';')

        # Consume noise
        while lex.consume('\\time') or lex.consume('\\t'):
            continue

        main_query = (SQLQuery.consume(lex) or
                      SQLDML.consume(lex) or
                      SQLCommand.consume(lex))

        if not main_query:
            return None

        if functions:
            return SQLWithFunctions(main_query, SQLNodeList(functions))

        return main_query
