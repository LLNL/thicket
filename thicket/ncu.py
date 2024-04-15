# Copyright 2022 Lawrence Livermore National Security, LLC and other
# Thicket Project Developers. See the top-level LICENSE file for details.
#
# SPDX-License-Identifier: MIT

from collections import defaultdict

from hatchet import QueryMatcher
import pandas as pd
from tqdm import tqdm

import ncu_report


class NCUReader:
    """Object to interface and pull NCU report data into Thicket"""

    rollup_operations = {
        None: None,
        ncu_report.IMetric.RollupOperation_AVG: pd.Series.mean,  # 1
        ncu_report.IMetric.RollupOperation_MAX: pd.Series.max,  # 2
        ncu_report.IMetric.RollupOperation_MIN: pd.Series.min,  # 3
        ncu_report.IMetric.RollupOperation_SUM: pd.Series.sum,  # 4
    }

    @staticmethod
    def _build_query_from_ncu_trace(kernel_call_trace):
        """Build QueryLanguage query from an NCU kernel call trace

        Arguments:
            kernel_call_trace (list): Call trace as seen from NCU
        """

        def _predicate_builder(kernel, is_regex=False):
            """Build predicate for QueryMatcher while forcing memoization

            Arguments:
                kernel (str): kernel name
                is_regex (bool): whether kernel is a regex

            Returns:
                predicate (function): predicate function
            """
            if is_regex:
                return (
                    lambda row: row["name"]
                    .apply(lambda x: kernel in x if x is not None else False)
                    .all()
                )
            else:
                return lambda row: row["name"].apply(lambda x: x == kernel).all()

        query = QueryMatcher()
        for i, kernel in enumerate(kernel_call_trace):
            if i == 0:
                query.match(".", _predicate_builder(kernel))
            elif i == len(kernel_call_trace) - 1:
                query.rel("*")
                query.rel(".", _predicate_builder(kernel, is_regex=True))
            else:
                query.rel(".", _predicate_builder(kernel))

        return query

    @staticmethod
    def _read_ncu(thicket, ncu_report_mapping):
        """Read NCU report files and return dictionary of data.

        Arguments:
            thicket (Thicket): thicket object to add ncu metrics to
            ncu_report_mapping (dict): mapping from NCU report file to profile

        Returns:
            data_dict (dict): dictionary of NCU data where key is tuple, (node, profile), mapping to list of dictionaries for per-rep data that is aggregated down to one dictionary.
        """

        # Initialize dict
        data_dict = defaultdict(list)
        rollup_dict = {}
        # Kernel mapping from NCU kernel to thicket node to save re-querying
        kernel_map = {}

        # Loop through NCU files
        for ncu_report_file in ncu_report_mapping:
            # NCU hash
            profile_mapping_flipped = {v: k for k, v in thicket.profile_mapping.items()}
            ncu_hash = profile_mapping_flipped[ncu_report_mapping[ncu_report_file]]

            # Load file
            report = ncu_report.load_report(ncu_report_file)

            # Error check
            if report.num_ranges() > 1:
                raise ValueError(
                    "NCU report file "
                    + ncu_report_file
                    + " has multiple ranges. Not supported yet."
                )
            # Loop through ranges in report
            for range in report:
                # Grab first action
                first_action = range.action_by_idx(0)
                # Metric names
                metric_names = [
                    first_action[name].name() for name in first_action.metric_names()
                ]
                # Setup rollup dict
                rollup_dict = {
                    name: first_action[name].rollup_operation() for name in metric_names
                }

                # Query action in range
                for action in tqdm(range):
                    # Name of kernel
                    kernel_name = action.name()
                    # Get NCU-side kernel trace
                    kernel_call_trace = list(
                        action.nvtx_state().domain_by_id(0).push_pop_ranges()
                    )

                    # Skip warmup kernels
                    if len(kernel_call_trace) == 0:
                        continue
                    else:
                        # Add kernel name to the end of the trace tuple
                        kernel_call_trace.append(kernel_name)

                        # Match ncu kernel to thicket node
                        matched_node = None
                        if kernel_name in kernel_map:
                            # Skip query building
                            matched_node = kernel_map[kernel_name]
                        else:  # kernel hasn't been seen yet
                            # Build query
                            query = NCUReader._build_query_from_ncu_trace(
                                kernel_call_trace
                            )
                            # Apply the query
                            node_set = query.apply(thicket)
                            # Find the correct node
                            matched_node = [
                                n for n in node_set if kernel_name in n.frame["name"]
                            ][0]

                        # matched_node should always exist at this point
                        assert matched_node is not None
                        # Set mapping
                        kernel_map[kernel_name] = matched_node

                        metric_values = [action[name].value() for name in metric_names]
                        assert len(metric_names) == len(metric_values)
                        data_dict[(matched_node, ncu_hash)].append(
                            dict(zip(metric_names, metric_values))
                        )

        return data_dict, rollup_dict
