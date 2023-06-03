# Copyright 2022 Lawrence Livermore National Security, LLC and other
# Thicket Project Developers. See the top-level LICENSE file for details.
#
# SPDX-License-Identifier: MIT

import re
from collections import defaultdict

import pandas as pd
import ncu_report


def add_ncu_metrics(th, ncu_report_mapping, chosen_metrics=None):
    """Add metrics from one ncu report file to one thicket

    Arguments:
        th (Thicket): Thicket object
        ncu_report_mapping (dict): dictionary mapping from "NCU report file":"Thicket
            profile"
        chosen_metrics (str): If known, which NCU metrics to add
    """
    # Keep list of NCU dfs for concat
    ncu_df_list = []

    # Loop through dict
    for ncu_report_file in ncu_report_mapping:
        # Hash value that should exist in th
        ncu_hash = hash(ncu_report_mapping[ncu_report_file])

        # Load kernels
        report = ncu_report.load_report(ncu_report_file)
        kernels = report[0]

        # Mapping from node names to kernels
        nodes = th.dataframe.index.get_level_values("node").tolist()
        names = th.dataframe["name"]
        cpu_side_kernels = {}
        for i in range(len(nodes)):
            if nodes[i].frame["type"] == "kernel":
                cpu_side_kernels[names[i]] = nodes[i]

        # Pre-processing
        # Remove warmup kernels
        warmup_end_idx = 0
        i = 0
        first_warmup_kernel = kernels[0]
        same_kernel = True
        for i in range(1, len(kernels)):
            if kernels[i].name(
                kernels[i].NameBase_DEMANGLED
            ) != first_warmup_kernel.name(first_warmup_kernel.NameBase_DEMANGLED):
                same_kernel = False
            if not same_kernel and kernels[i].name(
                kernels[i].NameBase_DEMANGLED
            ) == first_warmup_kernel.name(first_warmup_kernel.NameBase_DEMANGLED):
                warmup_end_idx = i
                break
        remove_warmup_kernels = kernels[warmup_end_idx:]
        # Remove duplicate kernels
        remove_dupe_kernels = []
        for kernel in remove_warmup_kernels:
            dupe = False
            for other_kernel in remove_dupe_kernels:
                if other_kernel.name(kernel.NameBase_DEMANGLED) == kernel.name(
                    kernel.NameBase_DEMANGLED
                ):
                    dupe = True
            if not dupe:
                remove_dupe_kernels.append(kernel)

        # Dictionary for metric values
        data_dict = defaultdict(list)
        # Matches everything between "<>"
        regex_str = r".*?\<(.*)\>.*"
        # For assertion
        first_kernel_metric_count = len(remove_dupe_kernels[0].metric_names())
        # Match kernels and add data
        for kernel in remove_dupe_kernels:
            kernel_name = kernel.name(kernel.NameBase_DEMANGLED)
            kernel_match = re.search(regex_str, kernel_name).group(1)
            ncu_side_kernel = kernel_name.replace(kernel_match, "").replace(" ", "")
            matches = []
            for other_kernel in cpu_side_kernels:
                k_match = re.search(regex_str, other_kernel).group(1)
                cpu_side_kernel = other_kernel.replace(k_match, "").replace(" ", "")
                if ncu_side_kernel == cpu_side_kernel:
                    matches.append(cpu_side_kernels[other_kernel])
                    # Remove entry since it should not be re-usable
                    cpu_side_kernels.pop(other_kernel)
                    break
            if len(matches) == 0:
                print(f"Could not match {kernel_name}")
                continue
            # Add metrics from NCU side
            data_dict["node"].append(matches[0])
            metrics = [kernel[name] for name in kernel.metric_names()]
            # Undefined behavior if this isn't true. We assume all kernels have same amount of metrics in the same order.
            assert len(metrics) == first_kernel_metric_count
            for metric in metrics:
                data_dict[metric.name()].append(metric.value())

        # Create NCU df
        ncu_df = pd.DataFrame.from_dict(data_dict)
        ncu_df["profile"] = ncu_hash
        ncu_df.set_index(["node", "profile"], inplace=True)

        # Get subset of metrics
        if chosen_metrics:
            ncu_df = ncu_df[chosen_metrics]

        # Append df to list
        ncu_df_list.append(ncu_df)

    # First join NCU dfs
    ncu_df = pd.concat(ncu_df_list)

    # Join Thicket and NCU dfs
    th.dataframe = th.dataframe.join(
        ncu_df,
        how="outer",
        sort=True,
        lsuffix="_left",
        rsuffix="_right",
    )
