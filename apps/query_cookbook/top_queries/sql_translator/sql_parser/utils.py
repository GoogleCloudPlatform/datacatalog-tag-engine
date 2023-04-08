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


from sql_translator.rfmt.blocks import LineBlock as LB
from sql_translator.rfmt.blocks import TextBlock as TB

from sql_translator.sql_parser.node import SQLNode


def comments_sqlf(comments):
    if not comments:
        return []
    return [TB('-- ' + c) for subc in comments for c in subc.split('\n')]


def with_commas(compact: bool, args, sep=',', tail=None):
    vargs = [a for a in args if isinstance(a, SQLNode)]

    if not vargs:
        return []

    with_commas = []
    for a in vargs[:-1]:
        with_commas.append(LB([a.sqlf(compact), TB(sep)]))
    if tail:
        with_commas.append(
            LB([vargs[-1].sqlf(compact), TB(tail)]))
    else:
        with_commas.append(vargs[-1].sqlf(compact))

    return with_commas
