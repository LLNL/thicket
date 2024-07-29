# Copyright 2022 Lawrence Livermore National Security, LLC and other
# Thicket Project Developers. See the top-level LICENSE file for details.
#
# SPDX-License-Identifier: MIT

from collections import defaultdict
import re

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
            # Set error check flag
            call_trace_found = False

            # NCU hash
            profile_mapping_flipped = {v: k for k, v in thicket.profile_mapping.items()}
            ncu_hash = profile_mapping_flipped[ncu_report_mapping[ncu_report_file]]

            # Relevant for kernel matching
            variant = thicket.metadata.loc[ncu_hash, "variant"]
            raja_lambda_cuda = variant.upper() == "RAJA_CUDA" or variant.upper() == "LAMBDA_CUDA"

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

                # Query action in range
                pbar = tqdm(range)
                for i, action in enumerate(pbar):
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
                        kernel_str = kernel_match.group(1)

                        if raja_lambda_cuda:
                            # RAJA_CUDA variant
                            instance_pattern = r"instance (\d+)"
                            instance_match = re.findall(
                                instance_pattern, demangled_kernel_name
                            )
                            instance_num = instance_match[-1]
                            kernel_name = kernel_str + "_" + instance_num
                        else:
                            # Base_CUDA variant
                            kernel_name = kernel_str

                        # Add kernel name to the end of the trace tuple
                        kernel_call_trace.append(kernel_str)

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
                            # Find the correct node. This may also get the parent so we take the last one
                            matched_nodes = [
                                n
                                for n in node_set
                                if kernel_str in n.frame["name"]
                                and (
                                    f"#{instance_num}" in n.frame["name"]
                                    if raja_lambda_cuda
                                    else True
                                )
                            ]
                            matched_node = matched_nodes[0]

                            if debug:
                                if not raja_lambda_cuda:
                                    instance_num = "NA"
                                print(
                                    f"Matched NCU kernel:\n\t{demangled_kernel_name}\nto Caliper Node:\n\t{matched_node}"
                                )
                                print(
                                    f"AKA:\n\t{kernel_str} (instance {instance_num}) == {kernel_str} (#{instance_num})\n"
                                )
                                print("All matched nodes:")
                                for node in matched_nodes:
                                    print("\t", node)

                        # Set mapping
                        kernel_map[kernel_name] = matched_node

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
