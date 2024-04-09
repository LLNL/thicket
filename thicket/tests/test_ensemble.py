# Copyright 2022 Lawrence Livermore National Security, LLC and other
# Thicket Project Developers. See the top-level LICENSE file for details.
#
# SPDX-License-Identifier: MIT

from thicket import Ensemble


def test_unify(literal_thickets):
    tk, tk2, tk3 = literal_thickets

    union_graph, _thickets = Ensemble._unify([tk, tk2, tk3], disable_tqdm=True)

    ug_hashes = [0, 1, 2, 3, 4, 5, 6]
    tk_hashes = [
        [0, 1, 2, 6],
        [0, 1, 2, 3],
        [3, 4, 5],
    ]

    assert [hash(n) for n in union_graph.traverse()] == ug_hashes
    assert [
        [hash(n) for n in _thickets[i].dataframe.index.get_level_values("node")]
        for i in range(3)
    ] == tk_hashes
