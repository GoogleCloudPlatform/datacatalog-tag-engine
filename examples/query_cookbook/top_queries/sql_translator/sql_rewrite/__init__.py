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


from .netezza import convert as netezza_convert
from .hive import convert as hive_convert
from .bigquery_cleanup import cleanup
from .stats import tables, tables_to_graph

MODES = [
    'NETEZZA',
    'HIVE',
    'CLEANUP',
]

def convert(mode, expr):
    if mode == 'HIVE':
        return hive_convert(expr)
    if mode == 'NETEZZA':
        return netezza_convert(expr)
    if mode == 'CLEANUP':
        return cleanup(expr)
    raise Exception('Invalid mode {}. Choices are ({}).'.format(
        mode, ', '.join(MODES)))
