# Copyright 2022 Lawrence Livermore National Security, LLC and other
# Thicket Project Developers. See the top-level LICENSE file for details.
#
# SPDX-License-Identifier: MIT

import pytest

import thicket as th


def test_mean(example_cali):
    th_ens = th.Thicket.from_caliperreader(example_cali)

    assert sorted(th_ens.dataframe.index.get_level_values(0).unique()) == sorted(th_ens.statsframe.dataframe.index.values)

    assert list(th_ens.statsframe.dataframe.columns) == ["name"]

    th.mean(th_ens, columns=["Min time/rank"])

    assert "Min time/rank_mean" in th_ens.statsframe.dataframe.columns
    # pr #42 needs to be merged before the below  asserts will work
    # assert "min#inclusive#sum#time.duration_mean" in th_ens.statsframe.exc_metrics + th_ens.statsframe.inc_metrics
    # assert "min#inclsuive#sum#time.duration_mean" in th_ens.statsframe.show_metric_columns()


def test_mean_columnar_join(columnar_join_thicket):
    thicket_list, thicket_list_cp, combined_th = columnar_join_thicket
    idx = combined_th.dataframe.columns.levels[0][0]
    assert sorted(combined_th.dataframe.index.get_level_values(0).unique()) == sorted(combined_th.statsframe.dataframe.index.values)

    assert list(combined_th.statsframe.dataframe.columns) == [("", "name")]

    #th.mean(combined_th, columns=[(idx, "Min time/rank")])

    #assert (idx, "Min time/rank_mean") in combined_th.statsframe.dataframe.columns
    # pr #42 needs to be merged before the below asserts will work
    # assert (idx, "min#inclusive#sum#time.duration_mean") in combined_th.statsframe.exc_metrics + th_ens.statsframe.inc_metrics
    # assert (idx, "min#inclusive#sum#time.duration_mean") in combined_th.stataframe.show_metric_columns()


def test_median(example_cali):
    th_ens = th.Thicket.from_caliperreader(example_cali)

    assert sorted(th_ens.dataframe.index.get_level_values(0).unique()) == sorted(th_ens.statsframe.dataframe.index.values)

    assert list(th_ens.statsframe.dataframe.columns) == ["name"]

    th.median(th_ens, columns=["Min time/rank"])

    assert "Min time/rank_median" in th_ens.statsframe.dataframe.columns
    # pr #42 needs to be merged before the below  asserts will work
    # assert "min#inclusive#sum#time.duration_median" in th_ens.statsframe.exc_metrics + th_ens.statsframe.inc_metrics
    # assert "min#inclsuive#sum#time.duration_median" in th_ens.statsframe.show_metric_columns()

def test_median_columnar_join(columnar_join_thicket):
    thicket_list, thicket_list_cp, combined_th = columnar_join_thicket
    idx = combined_th.dataframe.columns.levels[0][0]
    assert sorted(combined_th.dataframe.index.get_level_values(0).unique()) == sorted(combined_th.statsframe.dataframe.index.values)

    assert list(combined_th.statsframe.dataframe.columns) == [("", "name")]

    #th.median(combined_th, columns=[(idx, "Min time/rank")])

    #assert (idx, "Min time/rank_median") in combined_th.statsframe.dataframe.columns
    # pr #42 needs to be merged before the below asserts will work
    # assert (idx, "min#inclusive#sum#time.duration_median") in combined_th.statsframe.exc_metrics + th_ens.statsframe.inc_metrics
    # assert (idx, "min#inclusive#sum#time.duration_median") in combined_th.stataframe.show_metric_columns()


def test_minimum(example_cali):
    th_ens = th.Thicket.from_caliperreader(example_cali)

    assert sorted(th_ens.dataframe.index.get_level_values(0).unique()) == sorted(th_ens.statsframe.dataframe.index.values)

    assert list(th_ens.statsframe.dataframe.columns) == ["name"]

    th.minimum(th_ens, columns=["Min time/rank"])

    assert "Min time/rank_min" in th_ens.statsframe.dataframe.columns
    # pr #42 needs to be merged before the below  asserts will work
    # assert "min#inclusive#sum#time.duration_minimum" in th_ens.statsframe.exc_metrics + th_ens.statsframe.inc_metrics
    # assert "min#inclsuive#sum#time.duration_minimum" in th_ens.statsframe.show_metric_columns()


def test_minimum_columnar_join(columnar_join_thicket):
    thicket_list, thicket_list_cp, combined_th = columnar_join_thicket
    idx = combined_th.dataframe.columns.levels[0][0]
    assert sorted(combined_th.dataframe.index.get_level_values(0).unique()) == sorted(combined_th.statsframe.dataframe.index.values)

    assert list(combined_th.statsframe.dataframe.columns) == [("", "name")]

    #th.minimum(combined_th, columns=[(idx, "Min time/rank")])

    #assert (idx, "Min time/rank_min") in combined_th.statsframe.dataframe.columns
    # pr #42 needs to be merged before the below asserts will work
    # assert (idx, "min#inclusive#sum#time.duration_min") in combined_th.statsframe.exc_metrics + th_ens.statsframe.inc_metrics
    # assert (idx, "min#inclusive#sum#time.duration_min") in combined_th.stataframe.show_metric_columns()


def test_maximum(example_cali):
    th_ens = th.Thicket.from_caliperreader(example_cali)

    assert sorted(th_ens.dataframe.index.get_level_values(0).unique()) == sorted(th_ens.statsframe.dataframe.index.values)

    assert list(th_ens.statsframe.dataframe.columns) == ["name"]

    th.maximum(th_ens, columns=["Min time/rank"])

    assert "Min time/rank_max" in th_ens.statsframe.dataframe.columns
    # pr #42 needs to be merged before the below  asserts will work
    # assert "min#inclusive#sum#time.duration_minimum" in th_ens.statsframe.exc_metrics + th_ens.statsframe.inc_metrics
    # assert "min#inclsuive#sum#time.duration_minimum" in th_ens.statsframe.show_metric_columns()


def test_minimum_columnar_join(columnar_join_thicket):
    thicket_list, thicket_list_cp, combined_th = columnar_join_thicket
    idx = combined_th.dataframe.columns.levels[0][0]
    assert sorted(combined_th.dataframe.index.get_level_values(0).unique()) == sorted(combined_th.statsframe.dataframe.index.values)

    assert list(combined_th.statsframe.dataframe.columns) == [("", "name")]

    #th.maximum(combined_th, columns=[(idx, "Min time/rank")])

    #assert (idx, "Min time/rank_max") in combined_th.statsframe.dataframe.columns
    # pr #42 needs to be merged before the below asserts will work
    # assert (idx, "min#inclusive#sum#time.duration_median") in combined_th.statsframe.exc_metrics + th_ens.statsframe.inc_metrics
    # assert (idx, "min#inclusive#sum#time.duration_median") in combined_th.stataframe.show_metric_columns()


def test_std(example_cali):
    th_ens = th.Thicket.from_caliperreader(example_cali)

    assert sorted(th_ens.dataframe.index.get_level_values(0).unique()) == sorted(th_ens.statsframe.dataframe.index.values)

    assert list(th_ens.statsframe.dataframe.columns) == ["name"]

    th.std(th_ens, columns=["Min time/rank"])

    assert "Min time/rank_std" in th_ens.statsframe.dataframe.columns
    # pr #42 needs to be merged before the below asserts will work
    # assert "min#inclusive#sum#time.duration_std" in th_ens.statsframe.exc_metrics + th_ens.statsframe.inc_metrics
    # assert "min#inclsuive#sum#time.duration_std" in th_ens.statsframe.show_metric_columns()


def test_std_columnar_join(columnar_join_thicket):
    thicket_list, thicket_list_cp, combined_th = columnar_join_thicket
    idx = combined_th.dataframe.columns.levels[0][0]
    assert sorted(combined_th.dataframe.index.get_level_values(0).unique()) == sorted(combined_th.statsframe.dataframe.index.values)

    assert list(combined_th.statsframe.dataframe.columns) == [("", "name")]

    # th.std(combined_th. columns=[(idx, "min#inclusive#sum#time.duration")])

    # assert (idx, "min#inclusive#sum#time.duration_std") in combined_th.statsframe.dataframe.columns
    # pr #42 needs to be merged before the below asserts will work
    # assert (idx, "min#inclusive#sum#time.duration_std") in combined_th.statsframe.exc_metrics + th_ens.statsframe.inc_metrics
    # assert (idx, "min#inclusive#sum#time.duration_std") in combined_th.stataframe.show_metric_columns()


def test_percentiles(example_cali):
    th_ens = th.Thicket.from_caliperreader(example_cali)

    assert sorted(th_ens.dataframe.index.get_level_values(0).unique()) == sorted(th_ens.statsframe.dataframe.index.values)
    assert list(th_ens.statsframe.dataframe.columns) == ["name"]

    th.percentiles(th_ens, columns=["Min time/rank"])

    assert "Min time/rank_percentiles" in th_ens.statsframe.dataframe.columns
    assert len(th_ens.statsframe.dataframe["Min time/rank_percentiles"][0]) == 3
    # pr #42 needs to be merged before the below asserts will work
    # assert "min#inclusive#sum#time.duration_percentiles" in th_ens.statsframe.exc_metrics + th_ens.statsframe.inc_metrics
    # assert "min#inclsuive#sum#time.duration_percentiles" in th_ens.statsframe.show_metric_columns()


def test_percentiles_columnar_join(columnar_join_thicket):
    thicket_list, thicket_list_cp, combined_th = columnar_join_thicket
    idx = combined_th.dataframe.columns.levels[0][0]
    assert sorted(combined_th.dataframe.index.get_level_values(0).unique()) == sorted(combined_th.statsframe.dataframe.index.values)

    assert list(combined_th.statsframe.dataframe.columns) == [("", "name")]

    # th.std(combined_th. columns=[(idx, "min#inclusive#sum#time.duration")])

    # assert (idx, "min#inclusive#sum#time.duration_percentiles") in combined_th.statsframe.dataframe.columns
    # pr #42 needs to be merged before the below asserts will work
    # assert (idx, "min#inclusive#sum#time.duration_percentiles") in combined_th.statsframe.exc_metrics + th_ens.statsframe.inc_metrics
    # assert (idx, "min#inclusive#sum#time.duration_percentiles") in combined_th.stataframe.show_metric_columns()
