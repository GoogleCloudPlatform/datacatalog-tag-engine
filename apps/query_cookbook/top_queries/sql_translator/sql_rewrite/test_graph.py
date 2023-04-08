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

from .graph import minimise_graph

class TestGraph(unittest.TestCase):

    def compare_graph(self, full, expected_min):
        self.assertEqual(minimise_graph(full), expected_min)

    def test_graph(self):
        self.compare_graph(
            {
                'a': {'b', 'c'},
                'b': {'c', 'd'},
                'c': {'d', 'f'},
            },
            {
                'a': {'b'},
                'b': {'c'},
                'c': {'d', 'f'},
            },
        )
