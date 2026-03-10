# Copyright 2022 Lawrence Livermore National Security, LLC and other
# Thicket Project Developers. See the top-level LICENSE file for details.
#
# SPDX-License-Identifier: MIT
import os
import json
import math
from functools import lru_cache
from collections import defaultdict


class CompilerStaticInfoAdder:
    MAX_IR_METRICS = {"maxLiveSSAValues", "maxLoopDepth"}
    SUM_KRU_METRICS = {"SGPRs Spill", "VGPRs Spill"}

    def __init__(self, extraction_plugin_out_dir, trace_file):
        self.extraction_plugin_out_dir = extraction_plugin_out_dir
        self.trace_file = trace_file

    def read_trace_log(self):
        with open(self.trace_file, "r") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    yield json.loads(line)
                except json.JSONDecodeError:
                    continue

    def build_path_to_node(self, graph):
        path_to_node = {}

        def dfs(node, path_parts, visited):
            if node in visited:
                return
            visited.add(node)

            name = node.frame["name"]
            curr_path = path_parts + [name]
            path_to_node["/".join(curr_path)] = node

            for child in node.children:
                dfs(child, curr_path, visited)

        visited = set()
        for root in graph.roots:
            dfs(root, [], visited)

        return path_to_node

    @staticmethod
    @lru_cache(maxsize=None)
    def _load_module_functions_cached(extraction_plugin_out_dir, module_relative_path):
        module_relative_path = module_relative_path.lstrip("/")
        path = os.path.join(extraction_plugin_out_dir, module_relative_path)

        if not os.path.isfile(path):
            return {}

        try:
            with open(path, "r") as f:
                return json.load(f).get("Functions", {})
        except Exception:
            return {}

    def load_module_functions(self, module_relative_path):
        return self._load_module_functions_cached(
            self.extraction_plugin_out_dir,
            module_relative_path,
        )

    def walk_call_tree(
        self,
        func_name,
        module_relative_path,
        ir_accum,
        opt_accum,
        visited,
    ):
        key = (func_name, module_relative_path)
        if key in visited:
            return
        visited.add(key)

        entry = self.load_module_functions(module_relative_path).get(func_name)
        if not entry:
            return

        for metric, value in entry.get("IRStatistics", {}).items():
            if value is None:
                continue
            if metric in self.MAX_IR_METRICS:
                ir_accum[metric] = max(ir_accum[metric], value)
            else:
                ir_accum[metric] += value

        for metric, value in (
            entry.get("RemarkInformation", {}).get("RemarkStatistics", {}).items()
        ):
            if value is not None:
                opt_accum[metric] += value

        for callee in entry.get("CalledFunctions", []):
            self.walk_call_tree(
                callee,
                module_relative_path,
                ir_accum,
                opt_accum,
                visited,
            )

    def add_to_thicket(self, thicket):
        region_index = self.build_path_to_node(thicket.graph)

        region_roots = defaultdict(set)

        for entry in self.read_trace_log():
            region_path = entry.get("RegionPath")
            func_name = entry.get("Function")
            module_relative_path = entry.get("ModulePath")

            if not region_path or not func_name or not module_relative_path:
                continue

            region_roots[region_path].add((func_name, module_relative_path))

        for region_path, roots in region_roots.items():
            th_node = region_index.get(region_path)
            if th_node is None:
                print(f"Missing region path in thicket: {region_path}")
                continue

            ir_accum = defaultdict(int)
            opt_accum = defaultdict(int)
            kru_sum = defaultdict(int)
            kru_min = defaultdict(lambda: math.inf)
            kru_max = defaultdict(lambda: -math.inf)

            for func_name, module_relative_path in roots:
                entry = self.load_module_functions(module_relative_path).get(func_name)
                if not entry:
                    continue

                # KRU: aggregate only root functions for this region
                for metric, value in entry.get("KRUInformation", {}).items():
                    if value is None:
                        continue
                    if metric in self.SUM_KRU_METRICS:
                        kru_sum[f"{metric} (sum)"] += value
                    else:
                        kru_min[metric] = min(kru_min[metric], value)
                        kru_max[metric] = max(kru_max[metric], value)

                # IR + OPT: aggregate over full reachable call tree
                self.walk_call_tree(
                    func_name,
                    module_relative_path,
                    ir_accum,
                    opt_accum,
                    visited=set(),
                )

            for metric, value in ir_accum.items():
                thicket.dataframe.loc[th_node, metric] = value

            for metric, value in opt_accum.items():
                thicket.dataframe.loc[th_node, metric] = value

            for metric, value in kru_sum.items():
                thicket.dataframe.loc[th_node, metric] = value

            for metric in kru_min:
                if kru_min[metric] != math.inf:
                    thicket.dataframe.loc[th_node, f"{metric} (min)"] = kru_min[metric]
                    thicket.dataframe.loc[th_node, f"{metric} (max)"] = kru_max[metric]
