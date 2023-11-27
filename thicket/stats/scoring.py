import pandas as pd
import numpy as np

from ..utils import verify_thicket_structures
from .mean import mean
from .std import std
import math

def _scoring_1(means, stds, num_nodes):

    results = []

    for i in range(num_nodes):
        result = (means[0][1] - means[1][i])  * ( (stds[0][i] - stds[1][i]) / (np.abs(means[0][1] - means[1][i])))
        results.append(result)
    
    return results

def _scoring_2(means, stds, num_nodes):

    results = []

    for i in range(num_nodes):
        result = (means[0][1] - means[1][i]) + (stds[0][i] / means[0][i])  - (stds[1][i] / means[1][i])
        results.append(result)
    
    return results

def _scoring_3(means, stds, num_nodes):

    results = []

    for i in range(num_nodes):
        result = None
        try:        
            result = 0.25 * np.log(0.25 * ( (stds[0][i] ** 2 /stds[1][i] ** 2 ) + (stds[1][i] ** 2 /stds[0][i] ** 2))) +\
                            0.25 * ( (means[0][i] - means[1][i]) **2 / (stds[0][i] ** 2 + stds[1][i] ** 2) )
        except ZeroDivisionError:
            result = np.nan
        results.append(result)
    
    return results

def _scoring_4(means, stds, num_nodes):

    results = []

    for i in range(num_nodes):
        result = None
        try:
            result = 1 - math.sqrt((2 * stds[0][i] * stds[1][i]) / (stds[0][i] ** 2 + stds[1][i] ** 2)) *\
                math.exp(-0.25 * ( (means[0][i] - means[1][i])**2) / (stds[0][i] ** 2 + stds[1][i] ** 2))
        except ZeroDivisionError:
            result = np.nan
        results.append(result)
    
    return results

def score(thicket, columns, scoring_function):
    
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

    if thicket.dataframe.columns.nlevels != 2:
        raise ValueError(
                "Thicket passed in must be a columnar joined thicket"
            )

    # Right now I have only two columns going in since we only have two compilers,
    # But going forward I'm not sure we need this check. Or we can at least check if columns is at least 1
    if len(columns) != 2:
        raise ValueError(
            "Must specify two columns"
        )

    # Scoring across targets must have the same column
    if columns[0][1] != columns[1][1]:
        raise ValueError(
            "Columns to score must be the same column!"
        )

    verify_thicket_structures(thicket.dataframe, columns)

    # Note: Right now we are dealing with two columns because I am making the assumption
    # That the scoring will only be applied to two targets (clang vs. intel). We need to discuss
    # how scoring should work if there are three targets, ie, introducing a third target like gcc or
    # something like that. 
    
    means = [[], []]
    stds = [[], []]

    # Calculate means and stds, adds both onto statsframe
    mean(thicket, columns)
    std(thicket, columns)

    """
        This is where I would put in the logic to group targets if thicket object has more than two targets.
        example:
            If thicket has Clang, Intel, and gcc
            The scoring would be applied to all three as such:
            Score (Clang, Intel), (Clang, gcc), (Intel, gcc)

            All three would be appended to the statsframe

            Need discussion on this!
    """

    means[0] = thicket.statsframe.dataframe[(columns[0][0], "{}_mean".format(columns[0][1]))].to_list()
    means[1] = thicket.statsframe.dataframe[(columns[1][0], "{}_mean".format(columns[1][1]))].to_list()
    stds[0] = thicket.statsframe.dataframe[(columns[0][0], "{}_std".format(columns[0][1]))].to_list()
    stds[1] = thicket.statsframe.dataframe[(columns[1][0], "{}_std".format(columns[1][1]))].to_list()
    
    num_nodes = len(thicket.dataframe.index.get_level_values(0).unique())

    #This is where we call the scoring function that the user specified!
    resulting_scores = scoring_function(means, stds, num_nodes)
    
    # Statsframe column naming:
    #   (Scoring, columnName_target1_target2_scoringFunctionName)
    stats_frame_column_name = ("Scoring", "{}_{}_{}_{}".format(columns[0][1], columns[0][0], columns[1][0], scoring_function.__name__))

    thicket.statsframe.dataframe[stats_frame_column_name] = resulting_scores

    return


def scoring_1(thicket, columns):
    score(thicket, columns, _scoring_1)

def scoring_2(thicket, columns):
    score(thicket, columns, _scoring_2)

def scoring_3(thicket, columns):
    score(thicket, columns, _scoring_3)

def scoring_4(thicket, columns):
    score(thicket, columns, _scoring_4)
