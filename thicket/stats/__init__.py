# Copyright 2022 Lawrence Livermore National Security, LLC and other
# Thicket Project Developers. See the top-level LICENSE file for details.
#
# SPDX-License-Identifier: MIT

# make flake8 unused names in this file.
# flake8: noqa: F401

from .maximum import maximum
from .mean import mean
from .median import median
from .minimum import minimum
from .sum import sum
from .percentiles import percentiles
from .std import std
from .variance import variance
from .calc_boxplot_statistics import calc_boxplot_statistics
from .correlation_nodewise import correlation_nodewise
from .check_normality import check_normality
from .scoring import score_delta_mean_delta_stdnorm
from .scoring import score_delta_mean_delta_coefficient_of_variation
from .scoring import score_bhattacharyya
from .scoring import score_hellinger
from .preference import preference
from .calc_temporal_pattern import calc_temporal_pattern
from .distance import bhattacharyya_distance
from .distance import hellinger_distance
from .confidence_interval import confidence_interval


try:
    import seaborn as sns
except:
    print("Seaborn not found, so skipping imports of plotting in thicket.stats")
    print("To enable this plotting, install seaborn or thicket[plotting]")
else:
    from .display_boxplot import display_boxplot
    from .display_histogram import display_histogram
    from .display_heatmap import display_heatmap
    from .display_violinplot import display_violinplot_thicket
    from .display_violinplot import display_violinplot
