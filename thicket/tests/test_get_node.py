# Copyright 2022 Lawrence Livermore National Security, LLC and other
# Thicket Project Developers. See the top-level LICENSE file for details.
#
# SPDX-License-Identifier: MIT

import pytest


def test_get_node(literal_thickets):
    tk, tk2, tk3 = literal_thickets

    with pytest.raises(ValueError):
        foo = tk.get_node("Foo")

    baz = tk.get_node("Baz")

    # Check node properties
    assert baz.frame["name"] == "Baz"
    assert baz.frame["type"] == "function"
    assert baz._hatchet_nid == 0
