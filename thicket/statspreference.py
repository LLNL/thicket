# Copyright 2022 Lawrence Livermore National Security, LLC and other
# Thicket Project Developers. See the top-level LICENSE file for details.
#
# SPDX-License-Identifier: MIT

import pandas as pd
import numpy as np
from scipy.stats import ttest_ind_from_stats
from scipy.stats import t

from thicket import Thicket


class StatsPreference(Thicket):
    """Determines a preference between two different thickets."""

    def __init__(
        self,
        thicket1,
        thicket2,
        columns=[],
        alpha=None,
        name_of_thicket1=None,
        name_of_thicket2=None,
        metadata_column=None,
    ):
        """Create a new statspreference object.

        Arguments:
            thicket1 (thicket): one thicket which will be compared to thicket2
            thicket2 (thicket): second thicket which will be compared to thicket1
            columns (list): list of 1 or more numeric columns to perform t-test on.
                Columns must be located in the performance data table.
            name_of_thicket1 (str, optional): descriptive name for thicket1
            name_of_thicket2 (str, optional): descriptive name for thicket2
            alpha (float, optional): configure the statistical significance threshold.
                Default is set to 0.05.
        """
        self.thicket1 = thicket1
        self.thicket2 = thicket2
        self.columns = columns
        self.metadata_column = metadata_column

        if alpha is None:
            self.alpha = 0.05
        else:
            self.alpha = alpha

        if name_of_thicket1 is None and name_of_thicket2 is None:
            self.combined_th = self.thicket1.columnar_join(
                self.thicket2, self.metadata_column
            )
            self.name_of_thicket1 = self.combined_th.dataframe.columns.levels[0][0]
            self.name_of_thicket2 = self.combined_th.dataframe.columns.levels[0][1]
        else:
            self.name_of_thicket1 = name_of_thicket1
            self.name_of_thicket2 = name_of_thicket2
            self.combined_th = self.thicket1.columnar_join(
                self.thicket2,
                self.metadata_column,
                self.name_of_thicket1,
                self.name_of_thicket2,
            )

        self.nobs1 = self.thicket1.dataframe.groupby(level=0).size()[0]
        self.nobs2 = self.thicket2.dataframe.groupby(level=0).size()[0]
        self.tvalue = t.ppf(q=1 - self.alpha / 2, df=self.nobs1 + self.nobs2 - 2)

    def preference(self, comparison_func, extra_cols=None):
        """Determine a preference between two different thickets.

        Arguments:
            comparison_func (callable function): function taking in two values,
                compared using a comparison operator such as < or >
            extra_cols (list, optional): list of 1 or more columns to be added to the
                initial instantiated column list

        Returns:
            (Thicket): a new thicket object with new column for preference
        """

        if extra_cols is not None:
            for new_cols in extra_cols:
                if new_cols not in self.columns:
                    self.columns.append(new_cols)

        self._ttest()

        for col in (
            self.combined_th.statsframe.dataframe["preference"]
            .filter(regex="tstatistic")
            .columns.tolist()
        ):
            for comparison_col in list(
                filter(
                    lambda x: col.strip("_")[0] in x,
                    self.combined_th.statsframe.dataframe[
                        self.name_of_thicket1
                    ].columns.tolist(),
                )
            ):
                preference = []
                for index in range(0, len(self.combined_th.statsframe.dataframe)):
                    if (
                        self.combined_th.statsframe.dataframe["preference"][col][index]
                        > self.tvalue
                        or self.combined_th.statsframe.dataframe["preference"][col][
                            index
                        ]
                        < -1 * self.tvalue
                    ):

                        val1 = self.combined_th.statsframe.dataframe[
                            self.name_of_thicket1
                        ][comparison_col][index]
                        val2 = self.combined_th.statsframe.dataframe[
                            self.name_of_thicket2
                        ][comparison_col][index]
                        preference.append(comparison_func(val1, val2))
                    else:
                        preference.append("No preference")

                self.combined_th.statsframe.dataframe[
                    ("preference", comparison_col + "_preferred")
                ] = preference

        self.combined_th.statsframe.dataframei = (
            self.combined_th.statsframe.dataframe.sort_index(axis=1)
        )

    def _ttest(self):
        """Calculate t-statistic.

        This method uses the number of observations, mean, and standard deviation
        from thicket1 and thicket2 to compute the t-statistic. The t-statistic is
        compared to the t-value to determine if there is a preference between thicket1
        and thicket2.
        """
        self._calc_mean()
        self._calc_std()

        col_means = (
            self.combined_th.statsframe.dataframe[self.name_of_thicket1]
            .filter(regex="mean")
            .columns.tolist()
        )
        col_stds = (
            self.combined_th.statsframe.dataframe[self.name_of_thicket1]
            .filter(regex="std")
            .columns.tolist()
        )

        for col_mean, col_std, col_name in zip(col_means, col_stds, self.columns):
            tStatisticList = []
            for i in range(0, len(self.combined_th.statsframe.dataframe)):
                tStatistic = ttest_ind_from_stats(
                    mean1=self.combined_th.statsframe.dataframe[self.name_of_thicket1][
                        col_mean
                    ][i],
                    std1=self.combined_th.statsframe.dataframe[self.name_of_thicket1][
                        col_std
                    ][i],
                    nobs1=self.nobs1,
                    mean2=self.combined_th.statsframe.dataframe[self.name_of_thicket2][
                        col_mean
                    ][i],
                    std2=self.combined_th.statsframe.dataframe[self.name_of_thicket2][
                        col_std
                    ][i],
                    nobs2=self.nobs2,
                    equal_var=False,
                )

                tStatisticList.append(tStatistic.statistic)
            self.combined_th.statsframe.dataframe[
                ("preference", "tvalue")
            ] = self.tvalue
            self.combined_th.statsframe.dataframe[
                ("preference", col_name + "_tstatistic")
            ] = tStatisticList

        self.combined_th.statsframe.dataframe = (
            self.combined_th.statsframe.dataframe.sort_index(axis=1)
        )

    def _calc_mean(self):
        """Calculates the mean value per node for thicket1 and thicket2."""
        for column in self.columns:
            mean_thicket1 = []
            mean_thicket2 = []
            for node in pd.unique(
                self.combined_th.dataframe.reset_index()["node"].tolist()
            ):
                mean1 = np.mean(
                    self.combined_th.dataframe.loc[node][
                        (self.name_of_thicket1, column)
                    ]
                )
                mean2 = np.mean(
                    self.combined_th.dataframe.loc[node][
                        (self.name_of_thicket2, column)
                    ]
                )
                mean_thicket1.append(mean1)
                mean_thicket2.append(mean2)

            self.combined_th.statsframe.dataframe[
                (self.name_of_thicket1, column + "_mean")
            ] = mean_thicket1
            self.combined_th.statsframe.dataframe[
                (self.name_of_thicket2, column + "_mean")
            ] = mean_thicket2

        self.combined_th.statsframe.dataframe = (
            self.combined_th.statsframe.dataframe.sort_index(axis=1)
        )

    def _calc_std(self):
        """Calculates the standard deviation per node for thicket1 and thicket2."""
        for column in self.columns:
            std_thicket1 = []
            std_thicket2 = []
            for node in pd.unique(
                self.combined_th.dataframe.reset_index()["node"].tolist()
            ):

                std1 = np.std(
                    self.combined_th.dataframe.loc[node][
                        (self.name_of_thicket1, column)
                    ]
                )
                std2 = np.std(
                    self.combined_th.dataframe.loc[node][
                        (self.name_of_thicket2, column)
                    ]
                )
                std_thicket1.append(std1)
                std_thicket2.append(std2)

            self.combined_th.statsframe.dataframe[
                (self.name_of_thicket1, column + "_std")
            ] = std_thicket1
            self.combined_th.statsframe.dataframe[
                (self.name_of_thicket2, column + "_std")
            ] = std_thicket2

        self.combined_th.statsframe.dataframe = (
            self.combined_th.statsframe.dataframe.sort_index(axis=1)
        )
