# Copyright 2022 Lawrence Livermore National Security, LLC and other
# Thicket Project Developers. See the top-level LICENSE file for details.
#
# SPDX-License-Identifier: MIT

import thicket as th


def test_mean(example_cali):
    th_ens = th.Thicket.from_caliperreader(example_cali)

    assert sorted(th_ens.dataframe.index.get_level_values(0).unique()) == sorted(th_ens.statsframe.dataframe.index.values)

    assert list(th_ens.statsframe.dataframe.columns) == ["name"]

    th.mean(th_ens, columns=["min#inclusive#sum#time.duration"])

    assert "min#inclusive#sum#time.duration_mean" in th_ens.statsframe.dataframe.columns
    assert "min#inclusive#sum#time.duration_mean" in th_ens.statsframe.exc_metrics + th_ens.statsframe.inc_metrics

    # TODO Assert metrics are in show_metric_columns()


def test_mean_columnar_join(columnar_join_thicket):
    thicket_list, thicket_list_cp, combined_th = columnar_join_thicket
    assert sorted(combined_th.dataframe.index.get_level_values(0).unique()) == sorted(th_ens.statsframe.dataframe.index.values)

    assert list(combined_th.statsframe.dataframe.columns) == ["name"]

    th.mean(combined_th. columns=["min#inclusive#sum#time.duration"])

    assert "min#inclusive#sum#time.duration_mean" in combined_th.statsframe.dataframe.columns
    assert "min#inclusive#sum#time.duration_mean" in combined_th.statsframe.exc_metrics + th_ens.statsframe.inc_metrics

    # TODO Assert metrics are in show_metric_columns()
