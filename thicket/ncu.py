# Copyright 2022 Lawrence Livermore National Security, LLC and other
# Thicket Project Developers. See the top-level LICENSE file for details.
#
# SPDX-License-Identifier: MIT

from collections import defaultdict

from tqdm import tqdm

from hatchet import QueryMatcher
import ncu_report


class NCUReader:
    """Object to interface and pull NCU report data into Thicket"""

    def __init__(
        self,
        thicket,
        ncu_report_mapping,
        chosen_metrics=None,
    ):
        """Initialize NCUReader object

        Arguments:
            thicket (Thicket): Thicket object
            ncu_report_mapping (dict): mapping from NCU report file to profile
            chosen_metrics (list): list of metrics to sub-select from NCU report
        """
        self.thicket = thicket
        self.ncu_report_mapping = ncu_report_mapping
        self.chosen_metrics = chosen_metrics

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
                return lambda row: row["name"].apply(lambda x: kernel in x).all()
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

    def _read_ncu(self):
        """Read NCU report files and return dictionary of data.

        Arguments:
            self (NCUReader): NCUReader object

        Returns:
            data_dict (dict): dictionary of NCU data where key is tuple, (node, profile), mapping to list of dictionaries for per-rep data that is aggregated down to one dictionary.
        """

        # Initialize dict
        data_dict = defaultdict(list)
        # Kernel mapping from NCU kernel to thicket node to save re-querying
        kernel_map = {}

        # Loop through NCU files
        for ncu_report_file in self.ncu_report_mapping:
            # NCU hash
            profile_mapping_flipped = {
                v: k for k, v in self.thicket.profile_mapping.items()
            }
            ncu_hash = profile_mapping_flipped[self.ncu_report_mapping[ncu_report_file]]

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
                            node_set = query.apply(self.thicket)
                            # Find the correct node
                            matched_node = [
                                n for n in node_set if kernel_name in n.frame["name"]
                            ][0]

                        # matched_node should always exist at this point
                        assert matched_node is not None
                        # Set mapping
                        kernel_map[kernel_name] = matched_node

                        # Add kernel metrics to data_dict
                        metric_dict = {}
                        for metric in [action[name] for name in action.metric_names()]:
                            metric_dict[metric.name()] = metric.value()
                        data_dict[(matched_node, ncu_hash)].append(metric_dict)

        return data_dict
