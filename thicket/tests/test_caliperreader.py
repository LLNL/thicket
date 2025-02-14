# Copyright 2022 Lawrence Livermore National Security, LLC and other
# Thicket Project Developers. See the top-level LICENSE file for details.
#
# SPDX-License-Identifier: MIT

from thicket import Thicket


def test_from_caliperreader(rajaperf_seq_O3_1M_cali, intersection, fill_perfdata):
    """Sanity test a thicket object with known data."""
    tk = Thicket.from_caliperreader(
        rajaperf_seq_O3_1M_cali[0],
        intersection=intersection,
        fill_perfdata=fill_perfdata,
        disable_tqdm=True,
    )

    # Check the object type
    assert isinstance(tk, Thicket)

    # Check the resulting dataframe shape
    assert tk.dataframe.shape == (74, 14)

    # Check a value in the dataframe
    assert (
        tk.dataframe.loc[
            tk.dataframe.index.get_level_values(0)[0], "Avg time/rank"
        ].values[0]
        == 103.47638
    )


def test_node_ordering_from_caliper(caliper_ordered, intersection, fill_perfdata):
    """Check the order of output from the native Caliper reader by examining a known input with node order column."""

    tk = Thicket.from_caliperreader(
        caliper_ordered[0],
        intersection=intersection,
        fill_perfdata=fill_perfdata,
        disable_tqdm=True,
    )

    expected_order = [
        "main",
        "lulesh.cycle",
        "TimeIncrement",
        "LagrangeLeapFrog",
        "LagrangeNodal",
        "CalcForceForNodes",
        "CalcVolumeForceForElems",
        "IntegrateStressForElems",
        "CalcHourglassControlForElems",
        "CalcFBHourglassForceForElems",
        "LagrangeElements",
        "CalcLagrangeElements",
        "CalcKinematicsForElems",
        "CalcQForElems",
        "CalcMonotonicQForElems",
        "ApplyMaterialPropertiesForElems",
        "EvalEOSForElems",
        "CalcEnergyForElems",
        "CalcTimeConstraintsForElems",
    ]
    expected_data_order = [
        1.250952,
        1.229935,
        0.000085,
        1.229702,
        0.604766,
        0.566399,
        0.561237,
        0.161196,
        0.395344,
        0.239849,
        0.614079,
        0.175102,
        0.168127,
        0.136318,
        0.038575,
        0.299062,
        0.293046,
        0.190395,
        0.010707,
    ]

    # check if the rows are in the expected order
    for i in range(0, tk.dataframe.shape[0]):
        node_name = tk.dataframe.iloc[i]["name"]
        assert node_name == expected_order[i]
        node_data = tk.dataframe.iloc[i]["Total time"]
        assert node_data == expected_data_order[i]

    # check the tree ordering is correct as well
    tk.dataframe["hnid"] = [
        node._hatchet_nid for node in tk.graph.node_order_traverse()
    ]
    output = tk.tree(metric_column="hnid", render_header=False, precision=3)
    for i in tk.dataframe["hnid"].tolist():
        location = output.find(str(i) + ".000")
        assert location != -1
        output = output[location:]

    # test node ordering True for multiple profiles
    tk_multi = Thicket.from_caliperreader(
        caliper_ordered,
        intersection=intersection,
        fill_perfdata=fill_perfdata,
        disable_tqdm=True,
    )
    assert tk_multi.graph.node_ordering
