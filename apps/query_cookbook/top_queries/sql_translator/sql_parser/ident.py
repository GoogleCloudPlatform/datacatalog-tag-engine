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

"""SQL Lexer - parse basic constructs from a StringReader.

"""

import re

from dataclasses import dataclass
from typing import List
from typing import Optional

from sql_translator.rfmt.blocks import TextBlock as TB
from sql_translator.rfmt.blocks import WrapBlock as WB
from sql_translator.rfmt.blocks import LineBlock as LB

from sql_translator.sql_parser.node import SQLNodeList
from sql_translator.sql_parser.node import SQLNode
from sql_translator.sql_parser.expr import SQLExpr


@dataclass(frozen=True)
class SQLIdentifier(SQLNode):
    value: str

    UNQUOTED_REGEX = re.compile('^[a-zA-Z_][\.A-Za-z0-9_\-:]*$')
    UNQUOTED_LASTCHAR = re.compile('[a-zA-Z_0-9]')

    def sqlf(self, compact):
        del compact

        # Use unquoted string if possible
        if (SQLIdentifier.UNQUOTED_REGEX.match(self.value) and
            SQLIdentifier.UNQUOTED_LASTCHAR.match(self.value[-1])):
            return TB(self.value)

        # Add quoting if needed
        return TB('`' + self.value + '`')

        return TB(self.value)

    @staticmethod
    def consume(lex) -> 'Optional[SQLIdentifier]':
        word = lex.consume_identifier()
        if not word:
            return None
        return SQLIdentifier(word)

    @staticmethod
    def parse(lex) -> 'SQLIdentifier':
        return (SQLIdentifier.consume(lex) or
                lex.error('expected identifier'))


@dataclass(frozen=True)
class SQLIdentifierPath(SQLExpr):
    names: SQLNodeList[SQLIdentifier]

    def sqlf(self, compact):
        del compact  # Unused
        words = []
        for i in range(len(self.names)):
            words.append(self.names[i].sqlf(True))
            if i < (len(self.names)-1):
                words.append(TB('.'))
        return LB(words)

    @staticmethod
    def parse(lex) -> 'SQLIdentifierPath':
        names: List[SQLIdentifier] = []

        while True:
            if lex.consume('*'):
                ids: List[SQLIdentifier] = []
                if lex.consume('EXCEPT'):
                    lex.expect('(')
                    while True:
                        ids.append(SQLIdentifier.parse(lex))
                        if not lex.consume(','):
                            break
                    lex.expect(')')
                return SQLWildcardPath(SQLNodeList(names), SQLNodeList(ids))

            names.append(SQLIdentifier.parse(lex))

            if not lex.consume('.'):
                break

        return SQLIdentifierPath(SQLNodeList(names))


@dataclass(frozen=True)
class SQLWildcardPath(SQLIdentifierPath):
    names: SQLNodeList[SQLIdentifier]
    except_ids: SQLNodeList[SQLIdentifier]

    def sqlf(self, compact):
        words = []
        if self.names:
            words.append(TB('.'.join(self.names) + '.*'))
        else:
            words.append(TB('*'))
        if self.except_ids:
            words.append(TB(' '))
            words.append(TB('EXCEPT('))
            except_list = [TB(x + ',') for x in self.except_ids[:-1]]
            except_list.append(TB(self.except_ids[-1]))
            words.append(WB(except_list))
            words.append(TB(')'))
        return LB(words)
