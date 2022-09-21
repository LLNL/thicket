# Copyright 2022 Lawrence Livermore National Security, LLC and other
# Thicket Project Developers. See the top-level LICENSE file for details.
#
# SPDX-License-Identifier: MIT

from thicket import Thicket


def test_read_json(example_json):
    jgs = ""
    with open(example_json, "r") as f:
        jgs = f.read()
    gf = Thicket.from_json(jgs)

    assert len(gf.dataframe) == 3278
    assert len(gf.graph) == 29
    assert gf.metadata is not None
    assert gf.statsframe is not None
    assert gf.graph is gf.statsframe.graph


def test_write_json(example_json):
    jgs = ""
    with open(example_json, "r") as f:
        jgs = f.read()
    gf = Thicket.from_json(jgs)
    json_out = gf.to_json()

    assert "".join(sorted("".join(sorted(jgs.split())))) == "".join(
        sorted("".join(json_out.split()))
    )
