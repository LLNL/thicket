# Copyright 2022 Lawrence Livermore National Security, LLC and other
# Thicket Project Developers. See the top-level LICENSE file for details.
#
# SPDX-License-Identifier: MIT

import pandas as pd
import numpy as np

from ..utils import verify_thicket_structures


def calc_boxplot_statistics(thicket, columns=[], quartiles=[0.25, 0.5, 0.75], **kwargs):
    """Calculates boxplot five number summary and appends these values to the statsframe.

    The 5 number summary includes (1) minimum, (2) q1, (3) median, (4) q3, and (5) maximum.

    Arguments:
        thicket (thicket): thicket object
        columns (list): list of columns to perform boxplot five number summary on
        quartiles (list): list containing three values between 0 and 1 to cut the
            distribution into equal probabilities
    """
    if len(quartiles) != 3:
        raise ValueError(
            "The argument quartiles takes a list containing three numbers between 0 and 1."
        )

    if len(columns) == 0:
        raise ValueError(
            "To see a list of valid columns, run thicket.performance_cols."
        )

    verify_thicket_structures(
        thicket.dataframe, index=["node", "profile"], columns=columns
    )

    q_list = str(tuple(quartiles))

    for col in columns:
        boxplot_dict = {
            col + "_q1" + q_list: [],
            col + "_q2" + q_list: [],
            col + "_q3" + q_list: [],
            col + "_iqr" + q_list: [],
            col + "_lowerfence" + q_list: [],
            col + "_upperfence" + q_list: [],
            col + "_outliers" + q_list: [],
        }

        for node in pd.unique(thicket.dataframe.reset_index()["node"].tolist()):
            values = thicket.dataframe.loc[node][col].tolist()

            q = np.quantile(values, quartiles)
            q1 = q[0]
            median = q[1]
            q3 = q[2]

            iqr = q3 - q1
            lower_fence = q1 - (1.5 * iqr)
            upper_fence = q3 + (1.5 * iqr)

            boxplot_dict[col + "_q1" + q_list].append(q1)
            boxplot_dict[col + "_q2" + q_list].append(median)
            boxplot_dict[col + "_q3" + q_list].append(q3)
            boxplot_dict[col + "_iqr" + q_list].append(iqr)
            boxplot_dict[col + "_lowerfence" + q_list].append(lower_fence)
            boxplot_dict[col + "_upperfence" + q_list].append(upper_fence)

            profile = []
            for i in range(0, len(values)):
                if values[i] > upper_fence or values[i] < lower_fence:
                    profile.append(
                        thicket.dataframe.loc[node].reset_index()["profile"][i]
                    )
                else:
                    continue
            if not profile:
                profile = []
            boxplot_dict[col + "_outliers" + q_list].append(profile)

        df_box = pd.DataFrame(boxplot_dict)
        df_box["node"] = thicket.statsframe.dataframe.index.tolist()
        df_box.set_index("node", inplace=True)
        thicket.statsframe.dataframe = thicket.statsframe.dataframe.join(df_box)
