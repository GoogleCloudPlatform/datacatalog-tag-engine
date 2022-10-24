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

"""SQL Base Parser - parse basic constructs from a StringReader.

"""

#####################################################################
#
# General tools for for examining, consuming, and expecting elements
#

from typing import List


class ParsingError(Exception):
    pass


# 'IF' isn't reserved... treat it as a normal internal function.
RESERVED_WORDS = (['ALL', 'AND', 'ANY', 'ARRAY', 'AS', 'ASC',
                   'ASSERT_ROWS_MODIFIED', 'AT', 'BETWEEN', 'BY', 'CASE',
                   'CAST', 'COLLATE', 'CONTAINS', 'CREATE', 'CROSS',
                   'CUBE', 'CURRENT', 'DEFAULT', 'DEFINE', 'DESC',
                   'DISTINCT', 'ELSE', 'END', 'ENUM', 'ESCAPE', 'EXCEPT',
                   'EXCLUDE', 'EXISTS', 'EXTRACT', 'FALSE', 'FETCH',
                   'FOLLOWING', 'FOR', 'FROM', 'FULL', 'GROUP', 'GROUPING',
                   'GROUPS', 'HASH', 'HAVING', 'IGNORE', 'IN', 'INNER',
                   'INTERSECT', 'INTERVAL', 'INTO', 'IS', 'JOIN',
                   'LATERAL', 'LEFT', 'LIKE', 'LIMIT', 'LOOKUP', 'MERGE',
                   'NATURAL', 'NEW', 'NO', 'NOT', 'NULL', 'NULLS', 'OF',
                   'ON', 'OR', 'ORDER', 'OUTER', 'OVER', 'PARTITION',
                   'PRECEDING', 'PROTO', 'RANGE', 'RECURSIVE', 'RESPECT',
                   'RIGHT', 'ROLLUP', 'ROWS', 'SELECT', 'SET', 'SOME',
                   'STRUCT', 'TABLESAMPLE', 'THEN', 'TO', 'TREAT', 'TRUE',
                   'UNBOUNDED', 'UNION', 'UNNEST', 'USING', 'WHEN', 'MINUS',
                   'WHERE', 'WINDOW', 'WITH', 'WITHIN', 'QUALIFY', 'RECORDS'])


class SQLLexer:

    def __init__(self, sql_str):
        self._str = sql_str
        self._pos = 0
        self._comments = []

        self.consume_all_space()

    CONTEXT = 25

    def error(self, msg):

        # Find beginning and end of snippet.
        pos = self._pos
        linec = self._str[0:pos].count('\n') + 1
        line_start = 0
        if linec > 1:
            line_start = self._str[0:pos + 1].rindex('\n') + 1

        snippet_beg = int(max(line_start, pos - SQLLexer.CONTEXT))
        try:
            snippet_end = int(
                min(self._str[line_start:].index('\n') + line_start + 1,
                    pos + SQLLexer.CONTEXT))
        except ValueError:
            snippet_end = int(min(len(self._str), pos + SQLLexer.CONTEXT))

        # Raise exception
        raise ParsingError('\n'.join([
            f'{msg} at [{linec}:{pos - line_start + 1}]',
            f'Snippet: {self._str[snippet_beg:snippet_end]}',
            f'         {"-" * (pos - snippet_beg)}^'
        ]))

    def get_comments(self) -> List[str]:
        rcomments = self._comments
        self._comments = []
        return rcomments

    def peek(self, elem):
        """Peek for an element.

        Args:
          elem: Element. If a string, an identifier is searched for. Otherwise
              an operator.

        Returns:
          Returns True/False if the element is available to consume.
        """
        if isinstance(elem, list):
            rpos = self._pos
            for el in elem:
                if not self.peek(el):
                    self._pos = rpos
                    return False
                self._pos += len(el)
                self.consume_all_space()
            self._pos = rpos
            return True

        # If elem does not start with an alpha character,
        # just check if it matches the next available text
        if not elem[0].isalpha():
            return self._str[self._pos:self._pos + len(elem)] == elem

        # If it *doesn't* match the upper case of next available next,
        # then it doesn't match.
        if self._str[self._pos:self._pos + len(elem)].upper() != elem:
            return False

        # If this is the end of the string, it's a match
        if self._pos + len(elem) >= len(self._str):
            return True

        # Check the next character -- if it's not an alpha character,
        # and not a digit or underscore then we have a match.
        ch = self._str[self._pos + len(elem)]
        if not ch.isalpha() and not ch.isdigit() and ch != '_':
            return True

        return False

    def consume_any(self, elems):
        """Consume any of a list of elements.

        Args:
            elems: List of elements
                See peek() for what is an element
                Tested by consume(elem)

        Returns:
            Returns elem from elems that was consumed or None
        """
        for e in elems:
            if self.consume(e):
                return e
        return None

    def consume(self, elem):
        """Consume for an element or list of elements.

        Args:
          elem(s): Element or list of elements.
              See peek() for what is an element.
              If a list, then all elements must be consumed to be satisfied.

        Returns:
          Returns True/False if the element(s) is/are available to consume.
        """
        if isinstance(elem, list):
            rpos = self._pos
            for e in elem:
                if not self.consume(e):
                    self._pos = rpos
                    return None
            return elem

        if self.peek(elem):
            self._pos += len(elem)
            self.consume_all_space()
            return elem

        return None

    def expect(self, elem):
        return (self.consume(elem) or
                self.error('Expected "' + elem + '"'))

    def expect_end(self):
        if self._pos < len(self._str):
            self.error('Expected end')

    def consume_all_space(self):
        """Consume all space and comments.

        Returns:
          This moves the position forward to the next non-space non-comment.
        """
        while self._pos < len(self._str):

            if self._str[self._pos].isspace():
                self._pos += 1
                continue

            # It's a SQL comment
            if self._str[self._pos:self._pos + 2] == '--':
                start = self._pos + 2
                try:
                    move = self._str[self._pos:].index('\n')
                except ValueError:
                    move = len(self._str) - self._pos
                self._comments.append(self._str[start:start + move - 2].strip())
                self._pos += move
                continue

            # It's a hash comment
            if self._str[self._pos] == '#':
                try:
                    move = self._str[self._pos:].index('\n')
                except ValueError:
                    move = len(self._str) - self._pos
                self._pos += move
                continue

            # It's a /* */ comment
            if self._str[self._pos:self._pos + 2] == '/*':
                start = self._pos + 2
                while self._pos < len(self._str):
                    if self._str[self._pos:self._pos + 2] == '*/':
                        self._comments.append(
                            self._str[start:self._pos].strip())
                        self._pos += 2
                        break
                    self._pos += 1
                continue

            # Something else - stop now!
            break

    def consume_identifier(self):
        """Consume identifier.

        Returns:
          This returns an identifier (that's not reserved) or None if
          one isn't available.
        """
        if self._pos >= len(self._str):
            return None

        is_escaped = None
        ch = self._str[self._pos]
        if ch in ('`', '"'):
            is_escaped = ch

        if not (is_escaped or
                ch.isalpha() or
                ch == ':' or
                ch == '_'):
            return None

        pos = self._pos

        if is_escaped:
            pos += 1

        v = []

        if ch == ':':
            v.append(ch)
            pos += 1

        while pos < len(self._str):
            ch = self._str[pos]
            if is_escaped:
                if ch == is_escaped:
                    break
            else:
                if not (ch.isalpha() or ch == '_' or ch.isdigit()):
                    break
            v.append(ch)
            pos += 1

        v = ''.join(v)
        if v.upper() in RESERVED_WORDS:
            return None

        if is_escaped:
            pos += 1

        self._pos = pos

        self.consume_all_space()

        return v

    def consume_number(self):
        """Consume number.

        Returns:
          This returns a number (float or integer) or None
          if one isn't available.
        """
        if self._pos == len(self._str):
            return None

        if not self._str[self._pos].isdigit():
            return None

        # Do the parsing itself.
        pos = self._pos
        is_frac = False
        divisor = 1
        num = 0
        while pos < len(self._str):
            ch = self._str[pos]
            if ch.isdigit():
                num *= 10
                num += int(ch)
                if is_frac:
                    divisor *= 10
            elif not is_frac and ch == '.':
                is_frac = True
            else:
                break
            pos += 1

        # TODO(scannell) need 'e'

        # TODO(scannell) This is awkward. Fix.
        self._pos = pos
        self.consume_all_space()

        if is_frac:
            return float(num) / divisor

        return num

    def consume_string(self):
        """Consume string.

        Returns:
          This returns a string or None if one isn't available.
        """
        spos = self._pos
        flag = None

        if spos == len(self._str):
            return None

        if self._str[spos] == 'b' or self._str[spos] == 'r':
            flag = self._str[spos]
            spos += 1

        if spos == len(self._str) or self._str[spos] not in ('"', '\''):
            return None

        if self._str[spos:spos + 3] == '"""':
            quotechar = self._str[spos:spos + 3]
        else:
            quotechar = self._str[spos]

        spos += len(quotechar)
        epos = spos
        while (epos < len(self._str) and
               self._str[epos:epos + len(quotechar)] != quotechar):
            epos += 1

        if epos == len(self._str):
            self.error('Expected string')

        self._pos = epos + len(quotechar)

        self.consume_all_space()

        return (self._str[spos:epos], flag)
