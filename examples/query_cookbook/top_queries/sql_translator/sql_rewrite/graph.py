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


def minimise_graph(graph):
    """graph is a dictionary of <tuple> -> set(<tuple>)

    This algorithm will remove any target <tuple> that also has
    an alternative indirect path.

    Example: (1) -> (2,3), (3) -> (4,5), (4) -> (2,6)

    The mapping for (1) will be simplified to (1) -> (3), as the
    2 target can be found through 3 -> 4, 4 -> 2.

    Returns: graph
    """

    # Exploded graph -- all connections direct and indirect
    egraph = dict()

    # For each source, find all targets
    def expand_all_source(src):

        # Already found -- leave it alone.
        if src in egraph:
            return

        # Create the initial new set.
        dest_set = set()
        egraph[src] = dest_set

        # If there are no targets for it, there's
        # nothing to expand..
        if not src in graph:
            return

        # Expand it as much as possible
        for dest in graph[src]:
            dest_set.add(dest)
            expand_all_source(dest)
            dest_set.update(egraph[dest])

        # In case there is a loop
        dest_set.discard(src)

    # Create a set of direct and indirect targets
    for src in graph:
        expand_all_source(src)

    # Now create the minimised set
    mgraph = dict()
    for src in egraph:
        children = list(egraph[src])

        # Want to iterate everything
        i = 0
        while i < len(children):
            j = 0
            while j < len(children):
                if j != i:
                    if (children[i] in egraph[children[j]] and
                        egraph[children[i]].issubset(egraph[children[j]])):
                        del children[i]
                        i -= 1
                        j = 0
                        break
                j += 1
            i += 1

        if children:
            mgraph[src] = set(children)

    return mgraph
