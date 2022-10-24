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


import re

from dataclasses import dataclass
from dataclasses import fields
from dataclasses import replace

from typing import Generic
from typing import TypeVar
from typing import Sequence
from typing import Any

from io import StringIO

import sql_translator.rfmt.base as base


# Default options for formatting SQL
_options: Any = base.Options()
_options.m0 = 0
_options.m1 = 100
_options.c0 = 0.05
_options.c1 = 240
_options.cb = 2
_options.cpack = 1e-3
_options.ind = 2
_options.force_brace = False
_options.space_arg_eq = True


@dataclass(frozen=True)
class SQLNode:

    def __str__(self):
        """Minified SQL for this node."""
        sqlstr = self.as_sql(True)
        # Remove comments
        sqlstr = re.sub(r'^\s*--[^\n]*\n', '', sqlstr)
        # Remove multispaces to a single space
        sqlstr = re.sub(r'\s+', ' ', sqlstr)
        # Needed twice for single-letter operators
        sqlstr = re.sub(r'([^\w\']) ([^-])', '\\1\\2', sqlstr)
        sqlstr = re.sub(r'([^\w\']) ([^-])', '\\1\\2', sqlstr)
        # Needed twice for single-letter operators
        sqlstr = re.sub(r'([^-]) ([^\w\'])', '\\1\\2', sqlstr)
        sqlstr = re.sub(r'([^-]) ([^\w\'])', '\\1\\2', sqlstr)
        # Remove space at end
        sqlstr = re.sub(r'\s*$', '', sqlstr)
        return sqlstr

    def get_tree(self):
        """Return compact representation of tree"""
        return repr(self)

    def as_sql(self, compact=False):
        """Fully formatted SQL for this node."""
        outp = StringIO()
        self.sqlf(compact).PrintOn(outp)
        return re.sub(r' *$', '', outp.getvalue(), flags=re.MULTILINE)

    def sqlf(self, compact):
        """Return format-blocks for this node."""
        del compact  # Unused
        raise Exception('Abstract method not implemented in {}.'.format(
                        self.get_name()))

    def get_name(self):
        return self.__class__.__name__

    def children(self):
        for field in fields(self):
            val = getattr(self, field.name)
            if val is None:
                continue
            if issubclass(type(val), SQLNode):
                yield (field.name, val)
            continue

    def rewrite_tree(self, fopt):
        nself = fopt(self) or self

        nvals = dict()
        for name, val in nself.children():
            nvals[name] = val.rewrite_tree(fopt)

        return replace(nself, **nvals)


SQLNodeType = TypeVar('SQLNodeType', bound=SQLNode)


class SQLNodeList(SQLNode, Generic[SQLNodeType], tuple):
    def __new__(cls, val: Sequence[SQLNodeType]):
        return tuple.__new__(cls, val)

    def __init__(self, val):
        del val  # Unused  (see __new__)
        super().__init__()

    def children(self):
        for i, child in enumerate(self):
            yield (i, child)

    def __repr__(self):
        return tuple.__repr__(self)

    def rewrite_tree(self, fopt):
        return SQLNodeList([f.rewrite_tree(fopt) for f in self])
