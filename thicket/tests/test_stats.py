# Copyright 2022 Lawrence Livermore National Security, LLC and other
# Thicket Project Developers. See the top-level LICENSE file for details.
#
# SPDX-License-Identifier: MIT

import thicket as th


def test_mean(rajaperf_seq_O3_1M_cali):
    th_ens = th.Thicket.from_caliperreader(rajaperf_seq_O3_1M_cali)

    assert sorted(th_ens.dataframe.index.get_level_values(0).unique()) == sorted(
        th_ens.statsframe.dataframe.index.values
    )

    assert list(th_ens.statsframe.dataframe.columns) == ["name"]

    th.stats.mean(th_ens, columns=["Min time/rank"])

    assert "Min time/rank_mean" in th_ens.statsframe.dataframe.columns
    assert (
        "Min time/rank_mean"
        in th_ens.statsframe.exc_metrics + th_ens.statsframe.inc_metrics
    )
    assert "Min time/rank_mean" in th_ens.statsframe.show_metric_columns()


def test_mean_columnar_join(thicket_axis_columns):
    thicket_list, thicket_list_cp, combined_th = thicket_axis_columns
    idx = combined_th.dataframe.columns.levels[0][0]
    assert sorted(combined_th.dataframe.index.get_level_values(0).unique()) == sorted(
        combined_th.statsframe.dataframe.index.values
    )

    assert list(combined_th.statsframe.dataframe.columns) == [("name", "")]

    th.stats.mean(combined_th, columns=[(idx, "Min time/rank")])

    assert (idx, "Min time/rank_mean") in combined_th.statsframe.dataframe.columns
    assert (
        idx,
        "Min time/rank_mean",
    ) in combined_th.statsframe.exc_metrics + combined_th.statsframe.inc_metrics
    assert (idx, "Min time/rank_mean") in combined_th.statsframe.show_metric_columns()


def test_median(rajaperf_seq_O3_1M_cali):
    th_ens = th.Thicket.from_caliperreader(rajaperf_seq_O3_1M_cali)

    assert sorted(th_ens.dataframe.index.get_level_values(0).unique()) == sorted(
        th_ens.statsframe.dataframe.index.values
    )

    assert list(th_ens.statsframe.dataframe.columns) == ["name"]

    th.stats.median(th_ens, columns=["Min time/rank"])

    assert "Min time/rank_median" in th_ens.statsframe.dataframe.columns
    assert (
        "Min time/rank_median"
        in th_ens.statsframe.exc_metrics + th_ens.statsframe.inc_metrics
    )
    assert "Min time/rank_median" in th_ens.statsframe.show_metric_columns()


def test_median_columnar_join(thicket_axis_columns):
    thicket_list, thicket_list_cp, combined_th = thicket_axis_columns
    idx = combined_th.dataframe.columns.levels[0][0]
    assert sorted(combined_th.dataframe.index.get_level_values(0).unique()) == sorted(
        combined_th.statsframe.dataframe.index.values
    )

    assert list(combined_th.statsframe.dataframe.columns) == [("name", "")]

    th.stats.median(combined_th, columns=[(idx, "Min time/rank")])

    assert (idx, "Min time/rank_median") in combined_th.statsframe.dataframe.columns
    assert (
        idx,
        "Min time/rank_median",
    ) in combined_th.statsframe.exc_metrics + combined_th.statsframe.inc_metrics
    assert (idx, "Min time/rank_median") in combined_th.statsframe.show_metric_columns()


def test_minimum(rajaperf_seq_O3_1M_cali):
    th_ens = th.Thicket.from_caliperreader(rajaperf_seq_O3_1M_cali)

    assert sorted(th_ens.dataframe.index.get_level_values(0).unique()) == sorted(
        th_ens.statsframe.dataframe.index.values
    )

    assert list(th_ens.statsframe.dataframe.columns) == ["name"]

    th.stats.minimum(th_ens, columns=["Min time/rank"])

    assert "Min time/rank_min" in th_ens.statsframe.dataframe.columns
    assert (
        "Min time/rank_min"
        in th_ens.statsframe.exc_metrics + th_ens.statsframe.inc_metrics
    )
    assert "Min time/rank_min" in th_ens.statsframe.show_metric_columns()


def test_minimum_columnar_join(thicket_axis_columns):
    thicket_list, thicket_list_cp, combined_th = thicket_axis_columns
    idx = combined_th.dataframe.columns.levels[0][0]
    assert sorted(combined_th.dataframe.index.get_level_values(0).unique()) == sorted(
        combined_th.statsframe.dataframe.index.values
    )

    assert list(combined_th.statsframe.dataframe.columns) == [("name", "")]

    th.stats.minimum(combined_th, columns=[(idx, "Min time/rank")])

    assert (idx, "Min time/rank_min") in combined_th.statsframe.dataframe.columns
    assert (
        idx,
        "Min time/rank_min",
    ) in combined_th.statsframe.exc_metrics + combined_th.statsframe.inc_metrics
    assert (idx, "Min time/rank_min") in combined_th.statsframe.show_metric_columns()


def test_maximum(rajaperf_seq_O3_1M_cali):
    th_ens = th.Thicket.from_caliperreader(rajaperf_seq_O3_1M_cali)

    assert sorted(th_ens.dataframe.index.get_level_values(0).unique()) == sorted(
        th_ens.statsframe.dataframe.index.values
    )

    assert list(th_ens.statsframe.dataframe.columns) == ["name"]

    th.stats.maximum(th_ens, columns=["Min time/rank"])

    assert "Min time/rank_max" in th_ens.statsframe.dataframe.columns
    assert (
        "Min time/rank_max"
        in th_ens.statsframe.exc_metrics + th_ens.statsframe.inc_metrics
    )
    assert "Min time/rank_max" in th_ens.statsframe.show_metric_columns()


def test_maximum_columnar_join(thicket_axis_columns):
    thicket_list, thicket_list_cp, combined_th = thicket_axis_columns
    idx = combined_th.dataframe.columns.levels[0][0]
    assert sorted(combined_th.dataframe.index.get_level_values(0).unique()) == sorted(
        combined_th.statsframe.dataframe.index.values
    )

    assert list(combined_th.statsframe.dataframe.columns) == [("name", "")]

    th.stats.maximum(combined_th, columns=[(idx, "Min time/rank")])

    assert (idx, "Min time/rank_max") in combined_th.statsframe.dataframe.columns
    assert (
        idx,
        "Min time/rank_max",
    ) in combined_th.statsframe.exc_metrics + combined_th.statsframe.inc_metrics
    assert (idx, "Min time/rank_max") in combined_th.statsframe.show_metric_columns()


def test_std(rajaperf_seq_O3_1M_cali):
    th_ens = th.Thicket.from_caliperreader(rajaperf_seq_O3_1M_cali)

    assert sorted(th_ens.dataframe.index.get_level_values(0).unique()) == sorted(
        th_ens.statsframe.dataframe.index.values
    )

    assert list(th_ens.statsframe.dataframe.columns) == ["name"]

    th.stats.std(th_ens, columns=["Min time/rank"])

    assert "Min time/rank_std" in th_ens.statsframe.dataframe.columns
    assert (
        "Min time/rank_std"
        in th_ens.statsframe.exc_metrics + th_ens.statsframe.inc_metrics
    )
    assert "Min time/rank_std" in th_ens.statsframe.show_metric_columns()


def test_std_columnar_join(thicket_axis_columns):
    thicket_list, thicket_list_cp, combined_th = thicket_axis_columns
    idx = combined_th.dataframe.columns.levels[0][0]
    assert sorted(combined_th.dataframe.index.get_level_values(0).unique()) == sorted(
        combined_th.statsframe.dataframe.index.values
    )

    assert list(combined_th.statsframe.dataframe.columns) == [("name", "")]

    th.stats.std(combined_th, columns=[(idx, "Min time/rank")])

    assert (idx, "Min time/rank_std") in combined_th.statsframe.dataframe.columns
    assert (
        idx,
        "Min time/rank_std",
    ) in combined_th.statsframe.exc_metrics + combined_th.statsframe.inc_metrics
    assert (idx, "Min time/rank_std") in combined_th.statsframe.show_metric_columns()


def test_percentiles(rajaperf_seq_O3_1M_cali):
    th_ens = th.Thicket.from_caliperreader(rajaperf_seq_O3_1M_cali)

    assert sorted(th_ens.dataframe.index.get_level_values(0).unique()) == sorted(
        th_ens.statsframe.dataframe.index.values
    )
    assert list(th_ens.statsframe.dataframe.columns) == ["name"]

    th.stats.percentiles(th_ens, columns=["Min time/rank"])

    assert "Min time/rank_percentiles_25" in th_ens.statsframe.dataframe.columns
    assert "Min time/rank_percentiles_50" in th_ens.statsframe.dataframe.columns
    assert "Min time/rank_percentiles_75" in th_ens.statsframe.dataframe.columns

    assert (
        "Min time/rank_percentiles_25"
        in th_ens.statsframe.exc_metrics + th_ens.statsframe.inc_metrics
    )
    assert (
        "Min time/rank_percentiles_50"
        in th_ens.statsframe.exc_metrics + th_ens.statsframe.inc_metrics
    )
    assert (
        "Min time/rank_percentiles_75"
        in th_ens.statsframe.exc_metrics + th_ens.statsframe.inc_metrics
    )
    assert "Min time/rank_percentiles_25" in th_ens.statsframe.show_metric_columns()
    assert "Min time/rank_percentiles_50" in th_ens.statsframe.show_metric_columns()
    assert "Min time/rank_percentiles_75" in th_ens.statsframe.show_metric_columns()


def test_percentiles_none(rajaperf_seq_O3_1M_cali):
    th_ens = th.Thicket.from_caliperreader(rajaperf_seq_O3_1M_cali)

    th.stats.percentiles(th_ens, columns=["Min time/rank"], percentiles=None)

    assert "Min time/rank_percentiles_25" in th_ens.statsframe.dataframe.columns
    assert "Min time/rank_percentiles_50" in th_ens.statsframe.dataframe.columns
    assert "Min time/rank_percentiles_75" in th_ens.statsframe.dataframe.columns


def test_percentiles_single_value(rajaperf_seq_O3_1M_cali):
    th_ens = th.Thicket.from_caliperreader(rajaperf_seq_O3_1M_cali)

    th.stats.percentiles(th_ens, columns=["Min time/rank"], percentiles=[0.3])

    assert "Min time/rank_percentiles_30" in th_ens.statsframe.dataframe.columns

    assert (
        "Min time/rank_percentiles_30"
        in th_ens.statsframe.exc_metrics + th_ens.statsframe.inc_metrics
    )

    assert "Min time/rank_percentiles_30" in th_ens.statsframe.show_metric_columns()


def test_percentiles_columnar_join(thicket_axis_columns):
    thicket_list, thicket_list_cp, combined_th = thicket_axis_columns
    idx = combined_th.dataframe.columns.levels[0][0]
    assert sorted(combined_th.dataframe.index.get_level_values(0).unique()) == sorted(
        combined_th.statsframe.dataframe.index.values
    )

    assert list(combined_th.statsframe.dataframe.columns) == [("name", "")]

    th.stats.percentiles(combined_th, columns=[(idx, "Min time/rank")])

    assert (
        idx,
        "Min time/rank_percentiles_25",
    ) in combined_th.statsframe.dataframe.columns
    assert (
        idx,
        "Min time/rank_percentiles_50",
    ) in combined_th.statsframe.dataframe.columns
    assert (
        idx,
        "Min time/rank_percentiles_75",
    ) in combined_th.statsframe.dataframe.columns
    assert (
        idx,
        "Min time/rank_percentiles_25",
    ) in combined_th.statsframe.exc_metrics + combined_th.statsframe.inc_metrics
    assert (
        idx,
        "Min time/rank_percentiles_50",
    ) in combined_th.statsframe.exc_metrics + combined_th.statsframe.inc_metrics
    assert (
        idx,
        "Min time/rank_percentiles_75",
    ) in combined_th.statsframe.exc_metrics + combined_th.statsframe.inc_metrics

    assert (
        idx,
        "Min time/rank_percentiles_25",
    ) in combined_th.statsframe.show_metric_columns()
    assert (
        idx,
        "Min time/rank_percentiles_50",
    ) in combined_th.statsframe.show_metric_columns()
    assert (
        idx,
        "Min time/rank_percentiles_75",
    ) in combined_th.statsframe.show_metric_columns()

    th.stats.percentiles(
        combined_th, columns=[(idx, "Min time/rank")], percentiles=[0.4]
    )

    assert (
        idx,
        "Min time/rank_percentiles_25",
    ) in combined_th.statsframe.dataframe.columns
    assert (
        idx,
        "Min time/rank_percentiles_50",
    ) in combined_th.statsframe.dataframe.columns
    assert (
        idx,
        "Min time/rank_percentiles_75",
    ) in combined_th.statsframe.dataframe.columns
    assert (
        idx,
        "Min time/rank_percentiles_25",
    ) in combined_th.statsframe.exc_metrics + combined_th.statsframe.inc_metrics
    assert (
        idx,
        "Min time/rank_percentiles_50",
    ) in combined_th.statsframe.exc_metrics + combined_th.statsframe.inc_metrics
    assert (
        idx,
        "Min time/rank_percentiles_75",
    ) in combined_th.statsframe.exc_metrics + combined_th.statsframe.inc_metrics

    assert (
        idx,
        "Min time/rank_percentiles_25",
    ) in combined_th.statsframe.show_metric_columns()
    assert (
        idx,
        "Min time/rank_percentiles_50",
    ) in combined_th.statsframe.show_metric_columns()
    assert (
        idx,
        "Min time/rank_percentiles_75",
    ) in combined_th.statsframe.show_metric_columns()

    th.stats.percentiles(
        combined_th, columns=[(idx, "Min time/rank")], percentiles=[0.4]
    )

    assert (
        idx,
        "Min time/rank_percentiles_40",
    ) in combined_th.statsframe.dataframe.columns
    assert (
        idx,
        "Min time/rank_percentiles_40",
    ) in combined_th.statsframe.exc_metrics + combined_th.statsframe.inc_metrics
    assert (
        idx,
        "Min time/rank_percentiles_40",
    ) in combined_th.statsframe.show_metric_columns()


def test_variance(rajaperf_seq_O3_1M_cali):
    th_ens = th.Thicket.from_caliperreader(rajaperf_seq_O3_1M_cali)

    assert sorted(th_ens.dataframe.index.get_level_values(0).unique()) == sorted(
        th_ens.statsframe.dataframe.index.values
    )

    assert list(th_ens.statsframe.dataframe.columns) == ["name"]

    th.stats.variance(th_ens, columns=["Min time/rank"])

    assert "Min time/rank_var" in th_ens.statsframe.dataframe.columns
    assert (
        "Min time/rank_var"
        in th_ens.statsframe.exc_metrics + th_ens.statsframe.inc_metrics
    )
    assert "Min time/rank_var" in th_ens.statsframe.show_metric_columns()


def test_variance_columnar_join(thicket_axis_columns):
    thicket_list, thicket_list_cp, combined_th = thicket_axis_columns
    idx = combined_th.dataframe.columns.levels[0][0]
    assert sorted(combined_th.dataframe.index.get_level_values(0).unique()) == sorted(
        combined_th.statsframe.dataframe.index.values
    )

    assert list(combined_th.statsframe.dataframe.columns) == [("name", "")]

    th.stats.variance(combined_th, columns=[(idx, "Min time/rank")])

    assert (idx, "Min time/rank_var") in combined_th.statsframe.dataframe.columns
    assert (
        idx,
        "Min time/rank_var",
    ) in combined_th.statsframe.exc_metrics + combined_th.statsframe.inc_metrics
    assert (idx, "Min time/rank_var") in combined_th.statsframe.show_metric_columns()


def test_normality(rajaperf_cuda_block128_1M_cali):
    th_ens = th.Thicket.from_caliperreader(rajaperf_cuda_block128_1M_cali)

    assert sorted(th_ens.dataframe.index.get_level_values(0).unique()) == sorted(
        th_ens.statsframe.dataframe.index.values
    )

    assert list(th_ens.statsframe.dataframe.columns) == ["name"]

    th.stats.check_normality(th_ens, columns=["Min time/rank"])

    assert th_ens.statsframe.dataframe["Min time/rank_normality"][0] in {
        "True",
        "False",
    }

    assert "Min time/rank_normality" in th_ens.statsframe.dataframe.columns
    assert (
        "Min time/rank_normality"
        in th_ens.statsframe.exc_metrics + th_ens.statsframe.inc_metrics
    )
    assert "Min time/rank_normality" in th_ens.statsframe.show_metric_columns()


def test_normality_columnar_join(thicket_axis_columns, stats_thicket_axis_columns):
    thicket_list, thicket_list_cp, combined_th = thicket_axis_columns
    sthicket_list, sthicket_list_cp, scombined_th = stats_thicket_axis_columns
    # new data must be added before uncommenting, need 3 or more datapoints
    # idx = combined_th.dataframe.columns.levels[0][0]
    assert sorted(combined_th.dataframe.index.get_level_values(0).unique()) == sorted(
        combined_th.statsframe.dataframe.index.values
    )

    assert list(combined_th.statsframe.dataframe.columns) == [("name", "")]

    for idx in ["Cuda 1", "Cuda 2"]:
        th.stats.check_normality(scombined_th, columns=[(idx, "Min time/rank")])

        assert (
            idx,
            "Min time/rank_normality",
        ) in scombined_th.statsframe.dataframe.columns
        assert (
            (
                idx,
                "Min time/rank_normality",
            )
            in scombined_th.statsframe.exc_metrics + scombined_th.statsframe.inc_metrics
        )
        assert (
            idx,
            "Min time/rank_normality",
        ) in scombined_th.statsframe.show_metric_columns()


def test_correlation(rajaperf_cuda_block128_1M_cali):
    th_ens = th.Thicket.from_caliperreader(rajaperf_cuda_block128_1M_cali)

    assert sorted(th_ens.dataframe.index.get_level_values(0).unique()) == sorted(
        th_ens.statsframe.dataframe.index.values
    )

    assert list(th_ens.statsframe.dataframe.columns) == ["name"]

    th.stats.correlation_nodewise(
        th_ens, column1="Min time/rank", column2="Max time/rank", correlation="pearson"
    )

    assert (
        "Min time/rank_vs_Max time/rank pearson" in th_ens.statsframe.dataframe.columns
    )


def test_correlation_columnar_join(thicket_axis_columns):
    thicket_list, thicket_list_cp, combined_th = thicket_axis_columns
    idx = combined_th.dataframe.columns.levels[0]
    assert sorted(combined_th.dataframe.index.get_level_values(0).unique()) == sorted(
        combined_th.statsframe.dataframe.index.values
    )

    assert list(combined_th.statsframe.dataframe.columns) == [("name", "")]

    th.stats.correlation_nodewise(
        combined_th,
        column1=(idx[0], "Min time/rank"),
        column2=(idx[1], "Max time/rank"),
        correlation="spearman",
    )

    assert (
        "Union statistics",
        "Min time/rank_vs_Max time/rank spearman",
    ) in combined_th.statsframe.dataframe.columns


def test_boxplot(rajaperf_seq_O3_1M_cali):
    th_ens = th.Thicket.from_caliperreader(rajaperf_seq_O3_1M_cali)

    assert sorted(th_ens.dataframe.index.get_level_values(0).unique()) == sorted(
        th_ens.statsframe.dataframe.index.values
    )

    assert list(th_ens.statsframe.dataframe.columns) == ["name"]

    th.stats.calc_boxplot_statistics(
        th_ens, columns=["Min time/rank"], quartiles=[0.25, 0.50, 0.75]
    )

    assert "Min time/rank_q1(0.25, 0.5, 0.75)" in th_ens.statsframe.dataframe.columns
    assert "Min time/rank_q2(0.25, 0.5, 0.75)" in th_ens.statsframe.dataframe.columns
    assert "Min time/rank_q3(0.25, 0.5, 0.75)" in th_ens.statsframe.dataframe.columns
    assert "Min time/rank_iqr(0.25, 0.5, 0.75)" in th_ens.statsframe.dataframe.columns
    assert (
        "Min time/rank_lowerfence(0.25, 0.5, 0.75)"
        in th_ens.statsframe.dataframe.columns
    )
    assert (
        "Min time/rank_upperfence(0.25, 0.5, 0.75)"
        in th_ens.statsframe.dataframe.columns
    )
    assert (
        "Min time/rank_outliers(0.25, 0.5, 0.75)" in th_ens.statsframe.dataframe.columns
    )

    assert (
        "Min time/rank_q1(0.25, 0.5, 0.75)"
        in th_ens.statsframe.exc_metrics + th_ens.statsframe.inc_metrics
    )
    assert (
        "Min time/rank_q2(0.25, 0.5, 0.75)"
        in th_ens.statsframe.exc_metrics + th_ens.statsframe.inc_metrics
    )
    assert (
        "Min time/rank_q3(0.25, 0.5, 0.75)"
        in th_ens.statsframe.exc_metrics + th_ens.statsframe.inc_metrics
    )
    assert (
        "Min time/rank_iqr(0.25, 0.5, 0.75)"
        in th_ens.statsframe.exc_metrics + th_ens.statsframe.inc_metrics
    )
    assert (
        "Min time/rank_lowerfence(0.25, 0.5, 0.75)"
        in th_ens.statsframe.exc_metrics + th_ens.statsframe.inc_metrics
    )
    assert (
        "Min time/rank_upperfence(0.25, 0.5, 0.75)"
        in th_ens.statsframe.exc_metrics + th_ens.statsframe.inc_metrics
    )
    assert (
        "Min time/rank_outliers(0.25, 0.5, 0.75)"
        in th_ens.statsframe.exc_metrics + th_ens.statsframe.inc_metrics
    )

    assert (
        "Min time/rank_q1(0.25, 0.5, 0.75)" in th_ens.statsframe.show_metric_columns()
    )
    assert (
        "Min time/rank_q2(0.25, 0.5, 0.75)" in th_ens.statsframe.show_metric_columns()
    )
    assert (
        "Min time/rank_q3(0.25, 0.5, 0.75)" in th_ens.statsframe.show_metric_columns()
    )
    assert (
        "Min time/rank_iqr(0.25, 0.5, 0.75)" in th_ens.statsframe.show_metric_columns()
    )
    assert (
        "Min time/rank_lowerfence(0.25, 0.5, 0.75)"
        in th_ens.statsframe.show_metric_columns()
    )
    assert (
        "Min time/rank_upperfence(0.25, 0.5, 0.75)"
        in th_ens.statsframe.show_metric_columns()
    )
    assert (
        "Min time/rank_outliers(0.25, 0.5, 0.75)"
        in th_ens.statsframe.show_metric_columns()
    )


def test_boxplot_columnar_join(thicket_axis_columns):
    thicket_list, thicket_list_cp, combined_th = thicket_axis_columns
    idx = combined_th.dataframe.columns.levels[0][0]
    assert sorted(combined_th.dataframe.index.get_level_values(0).unique()) == sorted(
        combined_th.statsframe.dataframe.index.values
    )

    assert list(combined_th.statsframe.dataframe.columns) == [("name", "")]

    th.stats.calc_boxplot_statistics(
        combined_th, columns=[(idx, "Min time/rank")], quartiles=[0.25, 0.50, 0.75]
    )

    assert (
        idx,
        "Min time/rank_q1(0.25, 0.5, 0.75)",
    ) in combined_th.statsframe.dataframe.columns
    assert (
        idx,
        "Min time/rank_q2(0.25, 0.5, 0.75)",
    ) in combined_th.statsframe.dataframe.columns
    assert (
        idx,
        "Min time/rank_q3(0.25, 0.5, 0.75)",
    ) in combined_th.statsframe.dataframe.columns
    assert (
        idx,
        "Min time/rank_iqr(0.25, 0.5, 0.75)",
    ) in combined_th.statsframe.dataframe.columns
    assert (
        idx,
        "Min time/rank_lowerfence(0.25, 0.5, 0.75)",
    ) in combined_th.statsframe.dataframe.columns
    assert (
        idx,
        "Min time/rank_upperfence(0.25, 0.5, 0.75)",
    ) in combined_th.statsframe.dataframe.columns
    assert (
        idx,
        "Min time/rank_outliers(0.25, 0.5, 0.75)",
    ) in combined_th.statsframe.dataframe.columns

    assert (
        idx,
        "Min time/rank_q1(0.25, 0.5, 0.75)",
    ) in combined_th.statsframe.exc_metrics + combined_th.statsframe.inc_metrics
    assert (
        idx,
        "Min time/rank_q2(0.25, 0.5, 0.75)",
    ) in combined_th.statsframe.exc_metrics + combined_th.statsframe.inc_metrics
    assert (
        idx,
        "Min time/rank_q3(0.25, 0.5, 0.75)",
    ) in combined_th.statsframe.exc_metrics + combined_th.statsframe.inc_metrics
    assert (
        idx,
        "Min time/rank_iqr(0.25, 0.5, 0.75)",
    ) in combined_th.statsframe.exc_metrics + combined_th.statsframe.inc_metrics
    assert (
        idx,
        "Min time/rank_lowerfence(0.25, 0.5, 0.75)",
    ) in combined_th.statsframe.exc_metrics + combined_th.statsframe.inc_metrics
    assert (
        idx,
        "Min time/rank_upperfence(0.25, 0.5, 0.75)",
    ) in combined_th.statsframe.exc_metrics + combined_th.statsframe.inc_metrics
    assert (
        idx,
        "Min time/rank_outliers(0.25, 0.5, 0.75)",
    ) in combined_th.statsframe.exc_metrics + combined_th.statsframe.inc_metrics

    assert (
        idx,
        "Min time/rank_q1(0.25, 0.5, 0.75)",
    ) in combined_th.statsframe.show_metric_columns()
    assert (
        idx,
        "Min time/rank_q2(0.25, 0.5, 0.75)",
    ) in combined_th.statsframe.show_metric_columns()
    assert (
        idx,
        "Min time/rank_q3(0.25, 0.5, 0.75)",
    ) in combined_th.statsframe.show_metric_columns()
    assert (
        idx,
        "Min time/rank_iqr(0.25, 0.5, 0.75)",
    ) in combined_th.statsframe.show_metric_columns()
    assert (
        idx,
        "Min time/rank_lowerfence(0.25, 0.5, 0.75)",
    ) in combined_th.statsframe.show_metric_columns()
    assert (
        idx,
        "Min time/rank_upperfence(0.25, 0.5, 0.75)",
    ) in combined_th.statsframe.show_metric_columns()
    assert (
        idx,
        "Min time/rank_outliers(0.25, 0.5, 0.75)",
    ) in combined_th.statsframe.show_metric_columns()
