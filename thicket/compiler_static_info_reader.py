# Copyright 2022 Lawrence Livermore National Security, LLC and other
# Thicket Project Developers. See the top-level LICENSE file for details.
#
# SPDX-License-Identifier: MIT
import json


class CompilerStaticInfoReader:
    def __init__(self, preprocessed_file):
        self.preprocessed_file = preprocessed_file

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

    def add_to_thicket(self, thicket):
        # Expects flattened preprocessor output (run preprocessor.py with --flat):
        with open(self.preprocessed_file) as f:
            data = json.load(f)

        region_index = self.build_path_to_node(thicket.graph)

        for region_path, metrics in data.items():
            th_node = region_index.get(region_path)
            if th_node is None:
                continue

            for metric, value in metrics.items():
                thicket.dataframe.loc[th_node, metric] = value
