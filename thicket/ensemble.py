# Copyright 2022 Lawrence Livermore National Security, LLC and other
# Thicket Project Developers. See the top-level LICENSE file for details.
#
# SPDX-License-Identifier: MIT

from collections import OrderedDict

from hatchet import GraphFrame
import numpy as np
import pandas as pd

import thicket.helpers as helpers
from .utils import verify_sorted_profile, verify_thicket_structures


class Ensemble:
    """Operations pertaining to ensembling profiles."""

    def __init__(
        self,
        thickets,
    ):
        """Initialize an Ensemble object."""
        self.thickets = thickets

    def _unify_listwise(self, debug=False):
        """Unify a list of Thicket's graphs and dataframes

        Arguments:
            debug (bool): print debug statements

        Returns:
            union_graph (hatchet.Graph): unified graph
        """
        # variable to keep track of case where all graphs are the same
        same_graphs = True

        # GRAPH UNIFICATION
        union_graph = self.thickets[0].graph
        for i in range(1, len(self.thickets)):  # n-1 unions
            # Check to skip unnecessary computation. apply short circuiting with 'or'.
            if (
                union_graph is self.thickets[i].graph
                or union_graph == self.thickets[i].graph
            ):
                if debug:
                    print("Union Graph == thicket[" + str(i) + "].graph")
            else:
                if debug:
                    print("Unifying (Union Graph, " + str(i) + ")")
                same_graphs = False
                # Unify graph with current thickets graph
                union_graph = union_graph.union(self.thickets[i].graph)

        # If the graphs were all the same in the first place then there is no need to
        # apply any node mappings.
        if same_graphs:
            return union_graph

        # DATAFRAME MAPPING UPDATE
        for i in range(len(self.thickets)):  # n ops
            node_map = {}
            # Create a node map from current thickets graph to the union graph. This is
            # only valid once the union graph is complete.
            union_graph.union(self.thickets[i].graph, node_map)
            names = self.thickets[i].dataframe.index.names
            self.thickets[i].dataframe.reset_index(inplace=True)

            # Apply node_map mapping
            self.thickets[i].dataframe["node"] = (
                self.thickets[i]
                .dataframe["node"]
                .apply(lambda node: node_map[id(node)])
            )
            self.thickets[i].dataframe.set_index(names, inplace=True, drop=True)

        # After this point the graph and dataframe in each thicket is out of sync.
        # We could update the graph element in thicket to be the union graph but if the
        # user prints out the graph how do we annotate nodes only contained in one
        # thicket.
        return union_graph

    def _unify_pairwise(self, debug=False):
        """Unifies two thickets graphs and dataframes.

        Ensure self and other have the same graph and same node IDs. This may change the
        node IDs in the dataframe.

        Update the graphs in the graphframe if they differ.

        Arguments:
            debug (bool): print debug statements

        Returns:
            union_graph (hatchet.Graph): unified graph
        """

        def _unify_pair(first, second):
            """Unify two Thicket's graphs and dataframes

            Arguments:
                first (Thicket): first thicket
                second (Thicket): second thicket

            Returns:
                union_graph (hatchet.Graph): unified graph
            """
            # Check for the same object. Cheap operation since no graph walkthrough.
            if first.graph is second.graph:
                if debug:
                    print("same graph (object)")
                return first.graph

            # Check for the same graph structure. Need to walk through graphs *but should
            # still be less expensive then performing the rest of this function.*
            if first.graph == second.graph:
                if debug:
                    print("same graph (structure)")
                return first.graph

            if debug:
                print("different graph")

            node_map = {}
            union_graph = first.graph.union(second.graph, node_map)

            first_index_names = first.dataframe.index.names
            second_index_names = second.dataframe.index.names

            first.dataframe.reset_index(inplace=True)
            second.dataframe.reset_index(inplace=True)

            first.dataframe["node"] = first.dataframe["node"].apply(
                lambda x: node_map[id(x)]
            )
            second.dataframe["node"] = second.dataframe["node"].apply(
                lambda x: node_map[id(x)]
            )

            first.dataframe.set_index(first_index_names, inplace=True)
            second.dataframe.set_index(second_index_names, inplace=True)

            first.graph = union_graph
            second.graph = union_graph

            return union_graph

        union_graph = self.thickets[0].graph
        for i in range(len(self.thickets)):
            for j in range(i + 1, len(self.thickets)):
                if debug:
                    print("Unifying (" + str(i) + ", " + str(j) + "...")
                union_graph = _unify_pair(self.thickets[i], self.thickets[j])
        return union_graph

    def horizontal(
        self,
        header_list=None,
        column_name=None,
    ):
        """Join Thicket attributes horizontally. For DataFrames, this implies expanding
        in the column direction. New column multi-index will be created with columns
        under separate indexer headers.

        Arguments:
            header_list (list): List of headers to use for the new columnar multi-index
            column_name (str): Name of the column from the metadata table to join on. If
                no argument is provided, it is assumed that there is no profile-wise
                relationship between self and other.

        Returns:
            (Thicket): New ensembled Thicket object
        """

        def _create_multiindex_columns(df, upper_idx_name):
            """Helper function to create multi-index column names from a dataframe's
            current columns.

            Arguments:
            df (DataFrame): source dataframe
            upper_idx_name (String): name of the newly added index in the multi-index.
                Prepended before each column as a tuple.

            Returns:
                (list): list of new indicies generated from the source dataframe
            """
            new_idx = []
            for column in df.columns:
                new_tuple = (upper_idx_name, column)
                new_idx.append(new_tuple)
            return new_idx

        ###
        # Step 0A: Pre-check of data structures
        ###
        # Required/expected format of the data
        for th in self.thickets:
            verify_thicket_structures(th.dataframe, index=["node", "profile"])
            verify_thicket_structures(th.statsframe.dataframe, index=["node"])
            verify_thicket_structures(th.metadata, index=["profile"])
        # Check for column_name in metadata
        if column_name:
            for th in self.thickets:
                verify_thicket_structures(th.metadata, columns=[column_name])
        # Check length of profiles match
        for i in range(len(self.thickets) - 1):
            if len(self.thickets[i].profile) != len(self.thickets[i + 1].profile):
                raise ValueError(
                    "Length of all thicket profiles must match. {} != {}".format(
                        len(self.thickets[i].profile), len(self.thickets[i + 1].profile)
                    )
                )
        # Ensure all thickets profiles are sorted. Must be true when column_name=None to
        # guarantee performance data table and metadata table match up.
        if column_name is None:
            for th in self.thickets:
                verify_sorted_profile(th.dataframe)
                verify_sorted_profile(th.metadata)

        ###
        # Step 0B: Variable Initialization
        ###
        # Initialize combined thicket
        combined_th = self.thickets[0].deepcopy()
        # Use copies to be non-destructive
        thicket_list_cp = [th.deepcopy() for th in self.thickets]

        ###
        # Step 1: Unify the thickets
        ###
        # Unify graphs if "self" and "other" do not have the same graph
        union_graph = thicket_list_cp[0].graph
        for i in range(len(thicket_list_cp) - 1):
            if thicket_list_cp[i].graph != thicket_list_cp[i + 1].graph:
                union_graph = union_graph.union(thicket_list_cp[i + 1].graph)
        combined_th.graph = union_graph
        for i in range(len(thicket_list_cp)):
            # Set all graphs to the union graph
            thicket_list_cp[i].graph = union_graph
            # Necessary to change dataframe hatchet id's to match the nodes in the graph
            helpers._sync_nodes_frame(union_graph, thicket_list_cp[i].dataframe)
            # For tree diff. dataframes need to be sorted.
            thicket_list_cp[i].dataframe.sort_index(inplace=True)

        ###
        # Step 2: Join "self" & "other" performance data table
        ###
        # Create header list if not provided
        if header_list is None:
            header_list = [i for i in range(len(self.thickets))]

        # Update index to reflect performance data table index
        new_mappings = {}  # Dictionary mapping old profiles to new profiles
        if column_name is None:  # Create index from scratch
            new_profiles = [i for i in range(len(thicket_list_cp[0].profile))]
            for i in range(len(thicket_list_cp)):
                thicket_list_cp[i].metadata["new_profiles"] = new_profiles
                thicket_list_cp[i].add_column_from_metadata_to_ensemble(
                    "new_profiles", drop=True
                )
                thicket_list_cp[i].dataframe.reset_index(level="profile", inplace=True)
                new_mappings.update(
                    pd.Series(
                        thicket_list_cp[i]
                        .dataframe["new_profiles"]
                        .map(lambda x: (x, header_list[i]))
                        .values,
                        index=thicket_list_cp[i].dataframe["profile"],
                    ).to_dict()
                )
                thicket_list_cp[i].dataframe.drop("profile", axis=1, inplace=True)
                thicket_list_cp[i].dataframe.set_index(
                    "new_profiles", append=True, inplace=True
                )
                thicket_list_cp[i].dataframe.index.rename(
                    "profile", level="new_profiles", inplace=True
                )
        else:  # Change second-level index to be from metadata's "column_name" column
            for i in range(len(thicket_list_cp)):
                thicket_list_cp[i].add_column_from_metadata_to_ensemble(column_name)
                thicket_list_cp[i].dataframe.reset_index(level="profile", inplace=True)
                new_mappings.update(
                    pd.Series(
                        thicket_list_cp[i]
                        .dataframe[column_name]
                        .map(lambda x: (x, header_list[i]))
                        .values,
                        index=thicket_list_cp[i].dataframe["profile"],
                    ).to_dict()
                )
                thicket_list_cp[i].dataframe.drop("profile", axis=1, inplace=True)
                thicket_list_cp[i].dataframe.set_index(
                    column_name, append=True, inplace=True
                )
                thicket_list_cp[i].dataframe.sort_index(inplace=True)

        # Create tuple columns
        new_columns = [
            _create_multiindex_columns(th.dataframe, header_list[i])
            for i, th in enumerate(thicket_list_cp)
        ]
        # Clear old metrics (non-tuple)
        combined_th.exc_metrics.clear()
        combined_th.inc_metrics.clear()
        # Update inc/exc metrics
        for i in range(len(new_columns)):
            for col_tuple in new_columns[i]:
                if col_tuple[1] in thicket_list_cp[i].exc_metrics:
                    combined_th.exc_metrics.append(col_tuple)
                if col_tuple[1] in thicket_list_cp[i].inc_metrics:
                    combined_th.inc_metrics.append(col_tuple)
        # Update columns
        for i in range(len(thicket_list_cp)):
            thicket_list_cp[i].dataframe.columns = pd.MultiIndex.from_tuples(
                new_columns[i]
            )

        # Concat performance data table together
        combined_th.dataframe = pd.concat(
            [thicket_list_cp[i].dataframe for i in range(len(thicket_list_cp))],
            axis="columns",
            join="outer",
        )

        # Extract "name" columns to upper level
        nodes = list(set(combined_th.dataframe.reset_index()["node"]))
        for node in nodes:
            combined_th.dataframe.loc[node, "name"] = node.frame["name"]
        combined_th.dataframe.drop(
            columns=[(header_list[i], "name") for i in range(len(header_list))],
            inplace=True,
        )

        ###
        # Step 3: Join "self" & "other" metadata table
        ###
        # Update index to reflect performance data table index
        for i in range(len(thicket_list_cp)):
            thicket_list_cp[i].metadata.reset_index(drop=True, inplace=True)
        if column_name is None:
            for i in range(len(thicket_list_cp)):
                thicket_list_cp[i].metadata.index.set_names("profile", inplace=True)
        else:
            for i in range(len(thicket_list_cp)):
                thicket_list_cp[i].metadata.set_index(column_name, inplace=True)
                thicket_list_cp[i].metadata.sort_index(inplace=True)

        # Create multi-index columns
        for i in range(len(thicket_list_cp)):
            thicket_list_cp[i].metadata.columns = pd.MultiIndex.from_tuples(
                _create_multiindex_columns(thicket_list_cp[i].metadata, header_list[i])
            )

        # Concat metadata together
        combined_th.metadata = pd.concat(
            [thicket_list_cp[i].metadata for i in range(len(thicket_list_cp))],
            axis="columns",
            join="outer",
        )

        ###
        # Step 4: Update other Thicket objects.
        ###
        for i in range(1, len(thicket_list_cp)):
            combined_th.profile += thicket_list_cp[i].profile  # Update "profile" object
            combined_th.profile_mapping.update(
                thicket_list_cp[i].profile_mapping
            )  # Update "profile_mapping" object
        combined_th.profile = [new_mappings[prf] for prf in combined_th.profile]
        profile_mapping_cp = combined_th.profile_mapping.copy()
        for k, v in profile_mapping_cp.items():
            combined_th.profile_mapping[
                new_mappings[k]
            ] = combined_th.profile_mapping.pop(k)

        # Clear aggregated statistics table
        combined_th.statsframe = GraphFrame(
            graph=combined_th.graph,
            dataframe=helpers._new_statsframe_df(
                combined_th.dataframe, multiindex=True
            ),
        )
        combined_th.performance_cols = helpers._get_perf_columns(combined_th.dataframe)

        return combined_th

    def vertical(self, pairwise=False, superthicket=False):
        """Unify a list of thickets into a single thicket

        Arguments:
            pairwise (bool): use the pairwise implementation of unify (use if having
                issues)
            superthicket (bool): whether the result is a superthicket

        Returns:
            unify_graph (hatchet.Graph): unified graph,
            unify_df (DataFrame): unified dataframe,
            unify_exc_metrics (list): exclusive metrics,
            unify_inc_metrics (list): inclusive metrics,
            unify_metadata (DataFrame): unified metadata,
            unify_profile (list): profiles,
            unify_profile_mapping (dict): profile mapping
        """
        unify_graph = None
        if pairwise:
            unify_graph = self._unify_pairwise()
        else:
            unify_graph = self._unify_listwise()

        helpers._resolve_missing_indicies(self.thickets)

        # Unify dataframe
        unify_df = pd.DataFrame()
        unify_inc_metrics = []
        unify_exc_metrics = []
        unify_metadata = pd.DataFrame()
        unify_profile = []
        unify_profile_mapping = {}

        # Unification loop
        for i, th in enumerate(self.thickets):
            unify_inc_metrics.extend(th.inc_metrics)
            unify_exc_metrics.extend(th.exc_metrics)
            if len(th.metadata) > 0:
                curr_meta = th.metadata.copy()
                unify_metadata = pd.concat([curr_meta, unify_metadata])
            if th.profile is not None:
                unify_profile.extend(th.profile)
            if th.profile_mapping is not None:
                unify_profile_mapping.update(th.profile_mapping)
            unify_df = pd.concat([th.dataframe, unify_df])

        # Fill missing rows in dataframe with NaN's
        fill_value = np.nan
        unify_df = unify_df.reindex(
            pd.MultiIndex.from_product(unify_df.index.levels), fill_value=fill_value
        )
        # Replace NaN with None in string columns
        for col in unify_df.columns:
            if pd.api.types.is_string_dtype(unify_df[col].dtype):
                unify_df[col].replace(fill_value, None, inplace=True)

        # Operations specific to a superthicket
        if superthicket:
            unify_metadata.index.rename("thicket", inplace=True)

            # Process to aggregate rows of thickets with the same name.
            def _agg_function(obj):
                """Aggregate values in 'obj' into a set to remove duplicates."""
                if len(obj) <= 1:
                    return obj
                else:
                    _set = set(obj)
                    if len(_set) == 1:
                        return _set.pop()
                    else:
                        return _set

            unify_metadata = unify_metadata.groupby("thicket").agg(_agg_function)

        # Have metadata index match performance data table index
        unify_metadata.sort_index(inplace=True)

        # Sort by hatchet node id
        unify_df.sort_index(inplace=True)

        unify_inc_metrics = list(set(unify_inc_metrics))
        unify_exc_metrics = list(set(unify_exc_metrics))

        # Workaround for graph/df node id mismatch.
        # (n tree nodes) X (m df nodes) X (m)
        helpers._sync_nodes(unify_graph, unify_df)

        # Mutate into OrderedDict to sort profile hashes
        unify_profile_mapping = OrderedDict(sorted(unify_profile_mapping.items()))

        unify_parts = (
            unify_graph,
            unify_df,
            unify_exc_metrics,
            unify_inc_metrics,
            unify_metadata,
            unify_profile,
            unify_profile_mapping,
        )
        return unify_parts
