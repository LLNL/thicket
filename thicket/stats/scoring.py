import pandas as pd
import numpy as np

from ..utils import verify_thicket_structures
from .mean import mean
from .std import std
import math

def _scoring_1(thicket, means, stds):

    results = []

    # Get number of nodes that we are scoring
    num_nodes = len(thicket.dataframe.index.get_level_values(0).unique())

    for i in range(num_nodes):
        result = (means[0][1] - means[1][i])  * ( (stds[0][i] - stds[1][i]) / (np.abs(means[0][1] - means[1][i])))
        results.append(result)
    
    return results

def _scoring_2(thicket, means, stds):

    results = []

    # Get number of nodes that we are scoring
    num_nodes = len(thicket.dataframe.index.get_level_values(0).unique())

    for i in range(num_nodes):
        result = (means[0][1] - means[1][i]) + (stds[0][i] / means[0][i])  - (stds[1][i] / means[1][i])
        results.append(result)
    
    return results

def _scoring_3(thicket, means, stds):

    results = []

    # Get number of nodes that we are scoring
    num_nodes = len(thicket.dataframe.index.get_level_values(0).unique())

    for i in range(num_nodes):
        result = None
        try:        
            result = 0.25 * np.log(0.25 * ( (stds[0][i] ** 2 /stds[1][i] ** 2 ) + (stds[1][i] ** 2 /stds[0][i] ** 2))) +\
                            0.25 * ( (means[0][i] - means[1][i]) **2 / (stds[0][i] ** 2 + stds[1][i] ** 2) )
        except ZeroDivisionError:
            result = np.nan
        results.append(result)
    
    return results

def _scoring_4(thicket, means, stds):

    results = []

    # Get number of nodes that we are scoring
    num_nodes = len(thicket.dataframe.index.get_level_values(0).unique())

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


    # This is put in because of the fact that we are enforcing a columnar joined thicket
    for column in columns:
        if isinstance(column, tuple) is False:
            raise ValueError(
                "Columns listed in columns must be a tuple!"
            )
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

    means[0] = thicket.statsframe.dataframe[(columns[0][0], "{}_mean".format(columns[0][1]))].to_list()
    means[1] = thicket.statsframe.dataframe[(columns[1][0], "{}_mean".format(columns[1][1]))].to_list()
    stds[0] = thicket.statsframe.dataframe[(columns[0][0], "{}_std".format(columns[0][1]))].to_list()
    stds[1] = thicket.statsframe.dataframe[(columns[1][0], "{}_std".format(columns[1][1]))].to_list()
    
    #This is where we call the scoring function that the user specified!
    resulting_scores = scoring_function(thicket, means, stds)

    stats_frame_column_name = ("Scoring", "{}_{}_{}_{}".format(columns[0][1], columns[0][0], columns[1][0], scoring_function.__name__))
    # print(resulting_scores)
    thicket.statsframe.dataframe[stats_frame_column_name] = resulting_scores


def scoring_1(thicket, columns):
    score(thicket, columns, _scoring_1)

def scoring_2(thicket, columns):
    score(thicket, columns, _scoring_2)

def scoring_3(thicket, columns):
    score(thicket, columns, _scoring_3)

def scoring_4(thicket, columns):
    score(thicket, columns, _scoring_4)
