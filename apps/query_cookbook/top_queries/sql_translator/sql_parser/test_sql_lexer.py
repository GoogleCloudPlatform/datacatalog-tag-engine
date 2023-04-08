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

from .lexer import SQLLexer
from .lexer import ParsingError


class TestLexer(unittest.TestCase):

    def test_end(self):

        # no expection
        SQLLexer('').expect_end()

        # expection
        with self.assertRaises(ParsingError):
            SQLLexer('not end').expect_end()

        # no expection
        SQLLexer('').expect_end()

    def check_expect(self, lex, elem, res):

        # Check basic
        self.assertEqual(SQLLexer(lex).consume(elem), res)

        # If we're expect None, then let's ensure expect works, too
        if res is None:
            with self.assertRaises(ParsingError):
                SQLLexer(lex).expect(elem)

    def test_elements(self):
        self.check_expect('+', '+', '+')
        self.check_expect('+', '/', None)

        self.check_expect('if', 'IF', 'IF')
        self.check_expect('ifs', 'IF', None)
        self.check_expect('if2', 'IF', None)
        self.check_expect('if(', 'IF', 'IF')
        self.check_expect('+', 'IF', None)
        self.check_expect('i', 'IF', None)

    def check_identifier(self, lex, rep):

        # Pull out the identifier
        val = SQLLexer(lex).consume_identifier()

        # Same..
        self.assertEqual(val, rep)

    def test_identifier(self):
        self.check_identifier('thisIsIt', 'thisIsIt')
        self.check_identifier('`this IsIt`', 'this IsIt')
        self.check_identifier('myID+', 'myID')
        self.check_identifier('', None)
        self.check_identifier('2', None)
        self.check_identifier('ASSERT_ROWS_MODIFIED', None)

    def check_const(self, lex, sql):
        lex = SQLLexer(lex)
        val = lex.consume_identifier()
        self.assertEqual(val, sql)
        lex.consume_all_space()
        self.assertEqual(lex.consume_identifier(), None)

    def test_sqlcomment(self):
        self.check_const('  -- this is all space\nIdentifier', 'Identifier')
        self.check_const(
            '  -- this is all space\nAgain -- end of file\n -- again',
            'Again')

    def test_ccomment(self):
        self.check_const('  /* this is all space\n asdf */ \n varTable',
                         'varTable')

    def test_hashcomment(self):
        self.check_const('  /* this is all space\n asdf */ \n # comment \n'
                         'f10 # end of file',
                         'f10')

    def check_comments(self, lex, comments, sql):
        lex = SQLLexer(lex)
        val = lex.consume_identifier()
        self.assertEqual(val, sql)
        self.assertEqual(comments, lex.get_comments())
        lex.consume_all_space()
        self.assertEqual(lex.consume_identifier(), None)

    def test_comment_extract(self):
        self.check_comments('  -- comment 1\n  # comment 2..\n  /* okay */ '
                            'Identifier',
                            [u'comment 1', u'okay'],
                            'Identifier')
