import pandas as pd
import numpy as np

from ..utils import verify_thicket_structures
from .mean import mean
from .std import std
import math

def _scoring_1(means_1, means_2, stds_1, stds_2, num_nodes):

    results = []

    for i in range(num_nodes):
        result = None
        try:
            result = (means_1[i] - means_2[i]) * ((stds_1[i] - stds_2[i]) / (np.abs(means_1[i] - means_2[i])))
        except RuntimeWarning:
            print("Score 1 means's: ", means_1[i], means_2[i], i)
            result = np.nan
        results.append(result)

    return results

def _scoring_2(means_1, means_2, stds_1, stds_2, num_nodes):

    results = []

    for i in range(num_nodes):
        result = (means_1[i] - means_2[i]) + (stds_1[i] / means_1[i])  - (stds_2[i] / means_2[i])
        results.append(result)
    
    return results

def _scoring_3(means_1, means_2, stds_1, stds_2, num_nodes):

    results = []

    for i in range(num_nodes):
        result = None
        try:
            result = 0.25 * np.log(0.25 * ( (stds_1[i] ** 2 / stds_2[i] ** 2 ) + (stds_2[i] ** 2 /stds_1[i] ** 2) + 2) ) +\
                            0.25 * ( (means_1[i] - means_2[i]) **2 / (stds_1[i] ** 2 + stds_2[i] ** 2) )
        except ZeroDivisionError:
            print("Score 3 std's: ", stds_1[i], stds_2[i], i)
            result = np.nan
        results.append(result)
    
    return results

def _scoring_4(means_1, means_2, stds_1, stds_2, num_nodes):

    results = []

    for i in range(num_nodes):
        result = None
        try:
            result = 1 - math.sqrt((2 * stds_1[i] * stds_2[i]) / (stds_1[i] ** 2 + stds_2[i] ** 2)) *\
                math.exp(-0.25 * ( (means_1[i] - means_2[i])**2) / (stds_1[i] ** 2 + stds_2[i] ** 2))
        except ZeroDivisionError:
            print("Score 4 std's: ", stds_1[i], stds_2[i], i)
            result = np.nan
        results.append(result)
    
    return results

# Implement warning for user that NAN's were put in stats frame, and why

def score(thicket, columns, output_column_name, scoring_function):
    
    if isinstance(columns, list) is False:
        raise ValueError(
                "Columns must be a list!"
        )

    # Columns must be a tuples since we are dealing with columnar joined thickets
    for column in columns:
        if isinstance(column, tuple) is False:
            raise ValueError(
                "Columns listed in columns must be a tuple!"
            )

    if thicket.dataframe.columns.nlevels == 1:
        raise ValueError(
                "Thicket passed in must be a columnar joined thicket"
            )

    if len(columns) != 2:
        raise ValueError(
            "Must specify two columns"
        )

    num_nodes = len(thicket.dataframe.index.get_level_values(0).unique())

    if num_nodes < 2:
        raise ValueError(
            "Must have more than one data point per node to score with!"
        )

    verify_thicket_structures(thicket.dataframe, columns)

    # Calculate means and stds, adds both onto statsframe
    mean(thicket, columns)
    std(thicket, columns)

    # Grab means and stds calculated from above
    means_target1 = thicket.statsframe.dataframe[(columns[0][0], "{}_mean".format(columns[0][1]))].to_list()
    means_target2 = thicket.statsframe.dataframe[(columns[1][0], "{}_mean".format(columns[1][1]))].to_list()
    stds_target1 = thicket.statsframe.dataframe[(columns[0][0], "{}_std".format(columns[0][1]))].to_list()
    stds_target2 = thicket.statsframe.dataframe[(columns[1][0], "{}_std".format(columns[1][1]))].to_list()

    # Call the scoring function that the user specified
    resulting_scores = scoring_function(means_target1, means_target2, stds_target1, stds_target2, num_nodes)
    
    # User can specify a column name for the statsframe, otherwise default it to:
    #   "target1_column1_target2_column2_scoreFunctionName"
    stats_frame_column_name = None

    if output_column_name == None:
        stats_frame_column_name = ("Scoring", "{}_{}_{}_{}_{}".format(columns[0][0], columns[0][1], columns[1][0], columns[1][1], scoring_function.__name__))
    else:
        stats_frame_column_name = output_column_name

    thicket.statsframe.dataframe[stats_frame_column_name] = resulting_scores

    return

def scoring_1(thicket, columns, output_column_name = None):
    score(thicket, columns, output_column_name, _scoring_1)

def scoring_2(thicket, columns, output_column_name = None):
    score(thicket, columns, output_column_name,  _scoring_2)

def scoring_3(thicket, columns, output_column_name = None):
    score(thicket, columns, output_column_name, _scoring_3)

def scoring_4(thicket, columns, output_column_name = None):
    score(thicket, columns, output_column_name, _scoring_4)
