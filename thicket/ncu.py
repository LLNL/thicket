# Copyright 2022 Lawrence Livermore National Security, LLC and other
# Thicket Project Developers. See the top-level LICENSE file for details.
#
# SPDX-License-Identifier: MIT

from collections import defaultdict
import re

from thicket.query import Query
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
    def _read_ncu(thicket, ncu_report_mapping, debug=False):
        """Read NCU report files and return dictionary of data.

        Arguments:
            thicket (Thicket): thicket object to add ncu metrics to
            ncu_report_mapping (dict): mapping from NCU report file to profile
            debug (bool): whether to print debug statements

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
            rep = 0
            # Set error check flag
            call_trace_found = False

            # NCU hash
            profile_mapping_flipped = {v: k for k, v in thicket.profile_mapping.items()}
            ncu_hash = profile_mapping_flipped[ncu_report_mapping[ncu_report_file]]

            # Relevant for kernel matching
            variant = thicket.metadata.loc[ncu_hash, "variant"]
            raja_lambda_cuda = (
                variant.upper() == "RAJA_CUDA" or variant.upper() == "LAMBDA_CUDA"
            )

            # Load file
            report = ncu_report.load_report(ncu_report_file)

            # Error check
            num_ranges = report.num_ranges()
            if num_ranges > 1:
                raise ValueError(
                    "NCU report file "
                    + ncu_report_file
                    + " has multiple ranges. Not supported yet."
                )
            elif num_ranges == 0:
                raise ValueError(
                    "NCU report file " + ncu_report_file + " has no ranges (no data)."
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

                kernel_iter_dict = defaultdict(lambda: [0, 1])

                # Query action in range
                pbar = tqdm(range, disable=False)
                for i, action in enumerate(pbar):
                    defaulted = True
                    pbar.set_description(f"Processing action {i}/{len(range)}")
                    # Demangled name of kernel
                    demangled_kernel_name = action.name(
                        ncu_report.IAction.NameBase_DEMANGLED
                    )
                    # Get NCU-side kernel trace
                    kernel_call_trace = list(
                        action.nvtx_state().domain_by_id(0).push_pop_ranges()
                    )

                    # Skip warmup kernels
                    if len(kernel_call_trace) == 0:
                        continue
                    else:
                        call_trace_found = True

                        # Call trace with last element removed
                        # (last elem usually not useful for matching)
                        temp_call_trace = kernel_call_trace[:-1]
                        call_trace_str = "::".join([s.lower() for s in temp_call_trace])

                        # Pattern ends with ":" if RAJA_CUDA, "<" if Base_CUDA
                        kernel_pattern = rf"{call_trace_str}::(\w+)[<:]"
                        kernel_match = re.search(kernel_pattern, demangled_kernel_name)


                        defaulted = True
                        kernel_call_trace.append("cudaLaunchKernel")
                        if tuple(kernel_call_trace) in kernel_map:
                            if debug:
                                print("Using cached kernel mapping")
                            cuda_launch_children = kernel_map[tuple(kernel_call_trace)]
                        else:
                            query = NCUReader._build_query_from_ncu_trace(
                                kernel_call_trace
                            )
                            # Apply the query
                            node_set = query.apply(thicket)

                            cuda_launch_list = [n for n in node_set if "cudaLaunchKernel" in n.frame["name"]]
                            if len(cuda_launch_list) > 1:
                                raise ValueError("Multiple nodes matched. something went wrong")
                            cuda_launch = cuda_launch_list[0]
                            
                            cuda_launch_children = cuda_launch.children

                        if kernel_iter_dict[tuple(kernel_call_trace)][0] >= len(cuda_launch_children):
                            kernel_iter_dict[tuple(kernel_call_trace)][1] += 1
                            if debug:
                                print(kernel_call_trace)
                                print(f"resetting iterator to 0... Assuming next rep. (rep {kernel_iter_dict[tuple(kernel_call_trace)][1]}?)")
                            kernel_iter_dict[tuple(kernel_call_trace)][0] = 0
                            #print(kernel_map)
                            
                        matched_node = cuda_launch_children[kernel_iter_dict[tuple(kernel_call_trace)][0]]

                        kernel_iter_dict[tuple(kernel_call_trace)][0] += 1

                        # Set mapping
                        #kernel_map[kernel_name] = matched_node
                        kernel_map[tuple(kernel_call_trace)] = cuda_launch_children

                        metric_values = [action[name].value() for name in metric_names]

                        assert len(metric_names) == len(metric_values)
                        data_dict[(matched_node, ncu_hash)].append(
                            dict(zip(metric_names, metric_values))
                        )

            if not call_trace_found:
                raise ValueError(
                    f"No kernel call traces found in {ncu_report_file}.\nCheck you are enabling the NVTX Caliper service when running NCU."
                )

        return data_dict, rollup_dict
