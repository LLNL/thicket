# Copyright 2022 Lawrence Livermore National Security, LLC and other
# Thicket Project Developers. See the top-level LICENSE file for details.
#
# SPDX-License-Identifier: MIT

import numpy as np
import pandas as pd
import scipy.stats as stats

import thicket as th
from ..utils import verify_thicket_structures
from .stats_utils import cache_stats_op
from thicket.stats import mean


@cache_stats_op
def confidence_interval(thicket, columns=None, confidence_value=0.95):
    output_column_names = []
    
    mean_cols = th.stats.mean(thicket, columns=columns)
    std_cols = th.stats.std(thicket, columns=columns)
    sample_sizes = []
    z = stats.norm.ppf((1 + confidence_value) / 2)

    idx = pd.IndexSlice
    for node in thicket.graph.traverse():
        node_df = thicket.dataframe.loc[idx[node, :]]
        sample_sizes.append(len(node_df))

    for i in range(0, len(columns)):
        x = thicket.statsframe.dataframe[mean_cols[i]]
        s = thicket.statsframe.dataframe[std_cols[i]]
        n = sample_sizes

        c_p = x + (z * (s / np.sqrt(n)))
        c_m = x - (z * (s / np.sqrt(n)))
        
        out = list(zip(c_m, c_p))
        out = pd.Series(out, index=thicket.statsframe.dataframe.index)

        # If multi index, place below first level
        out_col = f"confidence_interval_{confidence_value}_{columns[i]}"
        output_column_names.append(out_col)
        thicket.statsframe.dataframe[out_col] = out
        break

    thicket.statsframe.dataframe = thicket.statsframe.dataframe.sort_index(axis=1)
    return output_column_names


