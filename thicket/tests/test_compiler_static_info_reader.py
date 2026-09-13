# Copyright 2022 Lawrence Livermore National Security, LLC and other
# Thicket Project Developers. See the top-level LICENSE file for details.
#
# SPDX-License-Identifier: MIT
import json

import numpy as np
import pytest

import thicket as th


def _build_path_to_node(graph):
    path_to_node = {}

    def dfs(node, path_parts, visited):
        if node in visited:
            return
        visited.add(node)

        curr_path = path_parts + [node.frame["name"]]
        path_to_node["/".join(curr_path)] = node

        for child in node.children:
            dfs(child, curr_path, visited)

    visited = set()
    for root in graph.roots:
        dfs(root, [], visited)

    return path_to_node


def test_add_compiler_static_info(
    rajaperf_hip_O2_32M_cali,
    rajaperf_hip_O2_32M_compiler_static_info,
):
    postprocessed_file = rajaperf_hip_O2_32M_compiler_static_info

    th_ens = th.Thicket.from_caliperreader(
        rajaperf_hip_O2_32M_cali,
        disable_tqdm=True,
    )

    with open(postprocessed_file) as file:
        compiler_static_info = json.load(file)

    path_to_node = _build_path_to_node(th_ens.graph)
    metric_names = {
        metric for metrics in compiler_static_info.values() for metric in metrics
    }

    assert set(compiler_static_info).issubset(path_to_node)

    for metric in metric_names:
        assert metric not in th_ens.dataframe.columns

    th_ens.add_compiler_static_info(postprocessed_file)

    for metric in metric_names:
        assert metric in th_ens.dataframe.columns

    for region_path, metrics in compiler_static_info.items():
        node = path_to_node[region_path]

        for metric, expected_value in metrics.items():
            actual_values = np.asarray(th_ens.dataframe.loc[node, metric], dtype=float)
            np.testing.assert_allclose(actual_values, expected_value)


def test_add_compiler_static_info_bad_input(
    rajaperf_hip_O2_32M_cali,
    rajaperf_hip_O2_32M_compiler_static_info,
):
    th_ens = th.Thicket.from_caliperreader(
        rajaperf_hip_O2_32M_cali,
        disable_tqdm=True,
    )

    with pytest.raises(TypeError, match="'preprocessed_file' must be a string path"):
        th_ens.add_compiler_static_info(None)

    with pytest.raises(FileNotFoundError, match="'preprocessed_file' does not exist"):
        th_ens.add_compiler_static_info(
            f"{rajaperf_hip_O2_32M_compiler_static_info}.missing"
        )


def test_add_compiler_static_info_columnar_ths(
    thicket_axis_columns,
    rajaperf_hip_O2_32M_compiler_static_info,
):
    postprocessed_file = rajaperf_hip_O2_32M_compiler_static_info
    _, _, combined_th = thicket_axis_columns

    with pytest.raises(ValueError, match="Concatenated Thicket detected"):
        combined_th.add_compiler_static_info(postprocessed_file)
