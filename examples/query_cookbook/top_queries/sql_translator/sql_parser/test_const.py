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

from .const import SQLConstant
from .lexer import SQLLexer


class TestLexer(unittest.TestCase):

    def check_const(self, ival, sql):
        lex = SQLLexer(ival)
        val = SQLConstant.consume(lex)
        self.assertEqual(val.as_sql(True), sql)
        self.assertEqual(val.as_sql(False), sql)
        self.assertEqual(str(val), sql)
        self.assertEqual(SQLConstant.consume(lex), None)

    def test_float(self):
        self.check_const('1234.42', '1234.42')

    def test_integer(self):
        self.check_const('1234', '1234')

    def test_quote(self):
        self.check_const('\'asdf\'', '\'asdf\'')
        self.check_const('"asdf"', '\'asdf\'')
        self.check_const('"""asdf"""', '\'asdf\'')

    def test_flag(self):
        self.check_const('b\'asdf\'', 'b\'asdf\'')

    def test_constant_type(self):
        self.check_const('true', 'TRUE')
        self.check_const('false', 'FALSE')
        self.check_const('null', 'NULL')
