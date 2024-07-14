import math

import numpy as np

from hatchet.util.perf_measure import annotate

import thicket as th
from ..utils import verify_thicket_structures
from .stats_utils import cache_stats_op


@annotate()
def _calc_score_delta_mean_delta_stdnorm(means_1, means_2, stds_1, stds_2, num_nodes):
    results = []

    for i in range(num_nodes):
        result = None
        try:
            result = (means_1[i] - means_2[i]) + (
                (stds_1[i] - stds_2[i]) / (np.abs(means_1[i] - means_2[i]))
            )
        except RuntimeWarning:
            result = np.nan
        results.append(result)

    return results


@annotate()
def _calc_score_delta_mean_delta_coefficient_of_variation(
    means_1, means_2, stds_1, stds_2, num_nodes
):
    results = []

    for i in range(num_nodes):
        result = (
            (means_1[i] - means_2[i])
            + (stds_1[i] / means_1[i])
            - (stds_2[i] / means_2[i])
        )
        results.append(result)
    return results


@annotate()
def _calc_score_bhattacharyya(means_1, means_2, stds_1, stds_2, num_nodes):
    results = []

    for i in range(num_nodes):
        result = None
        try:
            result = 0.25 * np.log(
                0.25
                * (
                    (stds_1[i] ** 2 / stds_2[i] ** 2)
                    + (stds_2[i] ** 2 / stds_1[i] ** 2)
                    + 2
                )
            ) + 0.25 * (
                (means_1[i] - means_2[i]) ** 2 / (stds_1[i] ** 2 + stds_2[i] ** 2)
            )
        except ZeroDivisionError:
            result = np.nan
        results.append(result)
    return results


@annotate()
def _calc_score_hellinger(means_1, means_2, stds_1, stds_2, num_nodes):
    results = []

    for i in range(num_nodes):
        result = None
        try:
            result = 1 - math.sqrt(
                (2 * stds_1[i] * stds_2[i]) / (stds_1[i] ** 2 + stds_2[i] ** 2)
            ) * math.exp(
                -0.25
                * ((means_1[i] - means_2[i]) ** 2)
                / (stds_1[i] ** 2 + stds_2[i] ** 2)
            )
        except ZeroDivisionError:
            result = np.nan
        results.append(result)
    return results


@annotate()
def score(thicket, columns, output_column_name, scoring_function):
    if isinstance(columns, list) is False:
        raise ValueError("Value passed to 'columns' must be of type list.")

    # Columns must be a tuples since we are dealing with columnar joined thickets
    for column in columns:
        if isinstance(column, tuple) is False:
            raise ValueError(
                "Columns listed in 'columns' argument must be of type tuple."
            )

    if not isinstance(thicket, th.Thicket):
        raise ValueError(
            "Value passed to 'thicket' argument must be of type thicket.Thicket."
        )

    if thicket.dataframe.columns.nlevels == 1:
        raise ValueError(
            "Value passed to 'thicket' argument must be a columnar joined thicket."
        )

    if len(columns) != 2:
        raise ValueError("Value passed to 'columns' argument must be of length 2.")

    num_nodes = len(thicket.dataframe.index.get_level_values(0).unique())

    if num_nodes < 2:
        raise ValueError("Must have more than one data point per node to score with!")

    verify_thicket_structures(thicket.dataframe, columns)

    output_column_names = []

    # Calculate means and stds, adds both onto statsframe
    th.stats.mean(thicket, columns)
    th.stats.std(thicket, columns)

    # Grab means and stds calculated from above
    means_target1 = thicket.statsframe.dataframe[
        (columns[0][0], "{}_mean".format(columns[0][1]))
    ].to_list()
    means_target2 = thicket.statsframe.dataframe[
        (columns[1][0], "{}_mean".format(columns[1][1]))
    ].to_list()
    stds_target1 = thicket.statsframe.dataframe[
        (columns[0][0], "{}_std".format(columns[0][1]))
    ].to_list()
    stds_target2 = thicket.statsframe.dataframe[
        (columns[1][0], "{}_std".format(columns[1][1]))
    ].to_list()

    # Call the scoring function that the user specified
    resulting_scores = scoring_function(
        means_target1, means_target2, stds_target1, stds_target2, num_nodes
    )
    # User can specify a column name for the statsframe, otherwise default it to:
    #   "target1_column1_target2_column2_scoreFunctionName"
    stats_frame_column_name = None

    if output_column_name is None:
        stats_frame_column_name = "{}_{}_{}_{}_{}".format(
            columns[0][0],
            columns[0][1],
            columns[1][0],
            columns[1][1],
            scoring_function.__name__,
        )
    else:
        stats_frame_column_name = output_column_name

    thicket.statsframe.dataframe["Scoring", stats_frame_column_name] = resulting_scores
    thicket.statsframe.dataframe = thicket.statsframe.dataframe.sort_index(axis=1)

    output_column_names.append(("Scoring", stats_frame_column_name))

    return output_column_names


@cache_stats_op
@annotate()
def score_delta_mean_delta_stdnorm(thicket, columns, output_column_name=None):
    r"""
    Apply a mean difference with standard deviation difference algorithm on two
    passed columns. The passed columns must be from the performance data table.

    Designed to take in a thicket object, specified columns, an output column name, and
    append the scoring to the thicket statsframe.

    This provides a quantitative way to compare two columns in terms of both their means
    and standard deviations. Yields a single value that represents the overall difference,
    considering both their central tendency and spread.

    Arguments:
        thicket (thicket)   : Thicket object
        columns (list)      : List of hardware/timing metrics to perform scoring on. A
            columnar joined thicket is required and as such  a list of tuples must be
            passed in with the format (column index, column name).
        output_column_name  : A string that assigns a name to the scoring column.

    Returns:
        (list): returns a list of output statsframe column names

    Equation:
        .. math::

            \text{result} = (\mu_1[i] - \mu_2[i]) + \frac{{\left(\sigma_1[i] - \sigma_2[i]\right)}}{{\left|\mu_1[i] - \mu_2[i]\right|}}
    \]
    """
    return score(
        thicket, columns, output_column_name, _calc_score_delta_mean_delta_stdnorm
    )


@cache_stats_op
@annotate()
def score_delta_mean_delta_coefficient_of_variation(
    thicket, columns, output_column_name=None
):
    r"""
    Apply a mean difference with difference spread of data algorithm on two passed columns.
    The passed columns must be from the performance data table.

    Designed to take in a thicket object, specified columns, an output column name, and
    append the scoring to the thicket statsframe.

    This provides a quantitative way to compare two columns through a measure that considers
    both the difference in means and the difference in spread of data (represented by the
    coefficient of variation).

    Arguments:
        thicket (thicket)   : Thicket object
        columns (list)      : List of hardware/timing metrics to perform scoring on. A
            columnar joined thicket is required and as such  a list of tuples must be
            passed in with the format (column index, column name).
        output_column_name  : A string that assigns a name to the scoring column.

    Returns:
        (list): returns a list of output statsframe column names

    Equation:
        .. math::

            \text{result} = (\mu_1[i] - \mu_2[i]) + \frac{{\sigma_1[i]}}{{\mu_1[i]}} - \frac{{\sigma_2[i]}}{{\mu_2[i]}}
    """
    return score(
        thicket,
        columns,
        output_column_name,
        _calc_score_delta_mean_delta_coefficient_of_variation,
    )


@cache_stats_op
@annotate()
def score_bhattacharyya(thicket, columns, output_column_name=None):
    r"""
    Apply the Bhattacharrya distance algorithm on two passed columns. The passed columns
    must be from the performance data table.

    Designed to take in a thicket object, specified columns, an output column name, and
    append the scoring to the thicket statsframe.

    This provides a quantitative way to compare two columns through the Bhattacharyya distance,
    which is a measure of the amount of overlap between two statistical samples or populations.
    It calculates the distance as a function of means and variances. It ranges from 0 to positive
    infinity, with 0 indicating complete overlap and vice versa. The larger the magnitude of the
    value, the larger the difference between the two comparisons.

    Arguments:
        thicket (thicket)   : Thicket object
        columns (list)      : List of hardware/timing metrics to perform scoring on. A
            columnar joined thicket is required and as such  a list of tuples must be
            passed in with the format (column index, column name).
        output_column_name  : A string that assigns a name to the scoring column.

    Returns:
        (list): returns a list of output statsframe column names

    Equation:
        .. math::

            \text{result} = \frac{1}{4} \cdot \log \left( \frac{1}{4} \cdot \left( \frac{{\sigma_1[i]^2}}{{\sigma_2[i]^2}} + \frac{{\sigma_2[i]^2}}{{\sigma_1[i]^2}} + 2 \right) \right) + \frac{1}{4} \cdot \left( \frac{{(\mu_1[i] - \mu_2[i])^2}}{{\sigma_1[i]^2 + \sigma_2[i]^2}} \right)
    """
    return score(thicket, columns, output_column_name, _calc_score_bhattacharyya)


@cache_stats_op
@annotate()
def score_hellinger(thicket, columns, output_column_name=None):
    r"""
    Apply the Hellinger's distance algorithm on two passed columns. The passed columns
    must be from the performance data table.

    Designed to take in a thicket object, specified columns, an output column name, and
    append the scoring to the thicket statsframe.

    This provides a quantitative way to compare two columns through the Hellinger distance,
    which is used to quantify the similarity between two probability distributions. It is based
    on comparing the square roots of the probability densities rather than the probabilites
    themselves. Helliger distance ranges from 0 to 1, with 0 indicating identical distributions
    and 1 indicating completely different distribution.

    Arguments:
        thicket (thicket)   : Thicket object.
        columns (list)      : List of hardware/timing metrics to perform scoring on. A
            columnar joined thicket is required and as such  a list of tuples must be
            passed in with the format (column index, column name).
        output_column_name  : A string that assigns a name to the scoring column.

    Returns:
        (list): returns a list of output statsframe column names

    Equation:

        .. math::

            \text{result} = 1 - \sqrt{\frac{{2 \sigma_1[i]\sigma_2[i]}}{{\sigma_1[i]^2 + \sigma_2[i]^2}}} \cdot \mathrm{e}^{-\frac{1}{4}\frac{{ (\mu_1[i] - \mu_2[i])^2}}{{\sigma_1[i]^2 + \sigma_2[i]^2}}}
    """
    return score(thicket, columns, output_column_name, _calc_score_hellinger)
