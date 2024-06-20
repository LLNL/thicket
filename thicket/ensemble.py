# Copyright 2022 Lawrence Livermore National Security, LLC and other
# Thicket Project Developers. See the top-level LICENSE file for details.
#
# SPDX-License-Identifier: MIT

from collections import OrderedDict
import warnings

from hatchet import GraphFrame
import numpy as np
import pandas as pd
import tqdm

import thicket.helpers as helpers
from .utils import (
    check_same_frame,
    validate_dataframe,
    verify_sorted_profile,
    verify_thicket_structures,
)


class Ensemble:
    """Operations pertaining to ensembling."""

    @staticmethod
    def _unify(thickets, inplace=False, disable_tqdm=False):
        """Create union graph from list of thickets and sync their DataFrames.

        Arguments:
            thickets (list): list of Thicket objects
            inplace (bool): whether to modify the original thicket objects or return new
            disable_tqdm (bool): whether to disable tqdm progress bar

        Returns:
            (tuple): tuple containing:
                (hatchet.Graph): unified graph
                (list): list of Thicket objects
        """

        def _merge_dicts(cur_dict, update_dict):
            """Merge old_to_new dictionary from the result of thicket i and i + 1 with old_to_new from i + 1 and i + 2.

            Arguments:
                cur_dict (dict): dictionary mapping old node to new node
                update_dict (dict): dictionary mapping old node to new node

            Returns:
                (dict): merged dictionary
            """
            merged_dict = {}

            if len(cur_dict) == 0:
                return update_dict

            # Merge values from update_dict into keys from cur_dict
            seen_keys = []
            for new_id, new_node in update_dict.items():
                for cur_id, cur_node in cur_dict.items():
                    if id(cur_node) == new_id:
                        merged_dict[cur_id] = new_node
                        seen_keys.append(new_id)

            # Pairs that are left in update_dict
            for tid, node in update_dict.items():
                if tid not in seen_keys:
                    merged_dict[tid] = node

            return merged_dict

        def _replace_graph_df_nodes(thickets, old_to_new, union_graph):
            """Replace the node objects in the graph and DataFrame of a Thicket object from the result of graph.union().

            Arguments:
                thickets (list): list of Thicket objects
                old_to_new (dict): dictionary mapping old node to new node
                union_graph (hatchet.Graph): unified graph

            Returns:
                (Thicket): modified Thicket object
            """
            for i in range(len(thickets)):
                thickets[i].graph = union_graph
                idx_names = thickets[i].dataframe.index.names
                thickets[i].dataframe = thickets[i].dataframe.reset_index()
                replace_dict = {}
                for node in thickets[i].dataframe["node"]:
                    node_id = id(node)
                    if node_id in old_to_new:
                        check_same_frame(node, old_to_new[node_id])
                        replace_dict[node] = old_to_new[node_id]
                thickets[i].dataframe["node"] = (
                    thickets[i].dataframe["node"].replace(replace_dict)
                )
                thickets[i].dataframe = thickets[i].dataframe.set_index(idx_names)

            return thickets

        _thickets = thickets
        if not inplace:
            _thickets = [th.deepcopy() for th in thickets]
        helpers._set_node_ordering(_thickets)
        # Unify graphs if "self" and "other" do not have the same graph
        union_graph = _thickets[0].graph
        old_to_new = {}
        pbar = tqdm.tqdm(range(len(_thickets) - 1), disable=disable_tqdm)
        for i in pbar:
            pbar.set_description("(2/2) Creating Thicket")
            temp_dict = {}
            union_graph = union_graph.union(_thickets[i + 1].graph, temp_dict)
            # Set all graphs to the union graph
            _thickets[i].graph = union_graph
            _thickets[i + 1].graph = union_graph
            # Merge the current old_to_new dictionary with the new mappings
            old_to_new = _merge_dicts(old_to_new, temp_dict)
        # Update the nodes in the dataframe
        _thickets = _replace_graph_df_nodes(_thickets, old_to_new, union_graph)
        for i in range(len(_thickets)):
            # For tree diff. dataframes need to be sorted.
            _thickets[i].dataframe.sort_index(inplace=True)
        return union_graph, _thickets

    @staticmethod
    def _columns(
        thickets,
        headers=None,
        metadata_key=None,
        disable_tqdm=False,
    ):
        """Concatenate Thicket attributes horizontally. For DataFrames, this implies expanding
        in the column direction. New column multi-index will be created with columns
        under separate indexer headers.

        Arguments:
            headers (list): List of headers to use for the new columnar multi-index
            metadata_key (str): Name of the column from the metadata tables to replace the 'profile'
                index. If no argument is provided, it is assumed that there is no profile-wise
                relationship between the thickets.
            disable_tqdm (bool): whether to disable tqdm progress bar

        Returns:
            (Thicket): New ensembled Thicket object
        """

        def _check_structures():
            """Check that the structures of the thicket objects are valid for the incoming operations."""
            # Required/expected format of the data
            for th in thickets:
                assert th.dataframe.index.nlevels == 2
                assert th.metadata.index.nlevels == 1
                assert th.dataframe.index.names[1] == th.metadata.index.name
                verify_thicket_structures(th.statsframe.dataframe, index=["node"])
            # Check for metadata_key in metadata
            if metadata_key:
                for th in thickets:
                    if metadata_key != th.metadata.index.name:
                        verify_thicket_structures(th.metadata, columns=[metadata_key])
            # Check length of profiles match if metadata key is not provided
            if metadata_key is None:
                for i in range(len(thickets) - 1):
                    if len(thickets[i].profile) != len(thickets[i + 1].profile):
                        raise ValueError(
                            f"Length of all thicket profiles must match if 'metadata_key' is not provided. {len(thickets[i].profile)} != {len(thickets[i + 1].profile)}"
                        )
            # Ensure all thickets profiles are sorted. Must be true when metadata_key=None to
            # guarantee performance data table and metadata table match up.
            if metadata_key is None:
                for th in thickets:
                    verify_sorted_profile(th.dataframe)
                    verify_sorted_profile(th.metadata)

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

        def _handle_metadata():
            """Handle operations to create new concatenated columnar axis metadata table."""
            # Update index to reflect performance data table index
            if metadata_key != inner_idx:
                for i in range(len(thickets_cp)):
                    thickets_cp[i].metadata.reset_index(drop=True, inplace=True)
            if metadata_key is None:
                for i in range(len(thickets_cp)):
                    thickets_cp[i].metadata.index.set_names("profile", inplace=True)
            else:
                for i in range(len(thickets_cp)):
                    if metadata_key != inner_idx:
                        thickets_cp[i].metadata.set_index(metadata_key, inplace=True)
                    thickets_cp[i].metadata.sort_index(inplace=True)

            # Create multi-index columns
            for i in range(len(thickets_cp)):
                thickets_cp[i].metadata.columns = pd.MultiIndex.from_tuples(
                    _create_multiindex_columns(thickets_cp[i].metadata, headers[i])
                )

            # Concat metadata together
            combined_th.metadata = pd.concat(
                [thickets_cp[i].metadata for i in range(len(thickets_cp))],
                axis="columns",
            )

        def _handle_misc():
            """Misceallaneous Thicket object operations."""
            for i in range(1, len(thickets_cp)):
                combined_th.profile += thickets_cp[i].profile  # Update "profile" object
                combined_th.profile_mapping.update(
                    thickets_cp[i].profile_mapping
                )  # Update "profile_mapping" object
            combined_th.profile = [new_mappings[prf] for prf in combined_th.profile]
            profile_mapping_cp = combined_th.profile_mapping.copy()
            for k, v in profile_mapping_cp.items():
                combined_th.profile_mapping[
                    new_mappings[k]
                ] = combined_th.profile_mapping.pop(k)
            combined_th.performance_cols = helpers._get_perf_columns(
                combined_th.dataframe
            )

        def _handle_perfdata():
            """Handle operations to create new concatenated columnar axis performance data table.

            Returns:
                (dict): dictionary mapping old profiles to new profiles
            """
            # Create header list if not provided
            nonlocal headers
            if headers is None:
                headers = [i for i in range(len(thickets))]

            # Update index to reflect performance data table index
            new_mappings = {}  # Dictionary mapping old profiles to new profiles
            if metadata_key is None:  # Create index from scratch
                new_profiles = [i for i in range(len(thickets_cp[0].profile))]
                for i in range(len(thickets_cp)):
                    thickets_cp[i].metadata["new_profiles"] = new_profiles
                    thickets_cp[i].metadata_column_to_perfdata(
                        "new_profiles", drop=True
                    )
                    thickets_cp[i].dataframe.reset_index(level=inner_idx, inplace=True)
                    new_mappings.update(
                        pd.Series(
                            thickets_cp[i]
                            .dataframe["new_profiles"]
                            .map(lambda x: (x, headers[i]))
                            .values,
                            index=thickets_cp[i].dataframe[inner_idx],
                        ).to_dict()
                    )
                    thickets_cp[i].dataframe.drop(inner_idx, axis=1, inplace=True)
                    thickets_cp[i].dataframe.set_index(
                        "new_profiles", append=True, inplace=True
                    )
                    thickets_cp[i].dataframe.index.rename(
                        "profile", level="new_profiles", inplace=True
                    )
            else:  # Change second-level index to be from metadata's "metadata_key" column
                for i in range(len(thickets_cp)):
                    if metadata_key not in thickets_cp[i].dataframe.index.names:
                        thickets_cp[i].metadata_column_to_perfdata(metadata_key)
                    thickets_cp[i].dataframe.reset_index(level=inner_idx, inplace=True)
                    new_mappings.update(
                        pd.Series(
                            thickets_cp[i]
                            .dataframe[metadata_key]
                            .map(lambda x: (x, headers[i]))
                            .values,
                            index=thickets_cp[i].dataframe[inner_idx],
                        ).to_dict()
                    )
                    if inner_idx != metadata_key:
                        thickets_cp[i].dataframe.drop(inner_idx, axis=1, inplace=True)
                    thickets_cp[i].dataframe.set_index(
                        metadata_key, append=True, inplace=True
                    )
                    thickets_cp[i].dataframe.sort_index(inplace=True)

            # Create tuple columns
            new_columns = [
                _create_multiindex_columns(th.dataframe, headers[i])
                for i, th in enumerate(thickets_cp)
            ]
            # Clear old metrics (non-tuple)
            combined_th.exc_metrics.clear()
            combined_th.inc_metrics.clear()
            # Update inc/exc metrics
            for i in range(len(new_columns)):
                for col_tuple in new_columns[i]:
                    if col_tuple[1] in thickets_cp[i].exc_metrics:
                        combined_th.exc_metrics.append(col_tuple)
                    if col_tuple[1] in thickets_cp[i].inc_metrics:
                        combined_th.inc_metrics.append(col_tuple)
            # Update columns
            for i in range(len(thickets_cp)):
                thickets_cp[i].dataframe.columns = pd.MultiIndex.from_tuples(
                    new_columns[i]
                )

            # Concat performance data table together
            combined_th.dataframe = pd.concat(
                [thickets_cp[i].dataframe for i in range(len(thickets_cp))],
                axis="columns",
            )

            # Extract "name" columns to upper level
            nodes = list(set(combined_th.dataframe.reset_index()["node"]))
            for node in nodes:
                combined_th.dataframe.loc[node, "name"] = node.frame["name"]
            combined_th.dataframe.drop(
                columns=[(headers[i], "name") for i in range(len(headers))],
                inplace=True,
            )

            # Sort DataFrame
            combined_th.dataframe.sort_index(inplace=True)

            return new_mappings

        def _handle_statsframe():
            """Handle operations to create new concatenated columnar axis aggregated statistics table."""
            # Clear aggregated statistics table
            combined_th.statsframe = GraphFrame(
                graph=combined_th.graph,
                dataframe=helpers._new_statsframe_df(
                    combined_th.dataframe, multiindex=True
                ),
            )

        # Step 0A: Variable Initialization
        combined_th = thickets[0].deepcopy()
        thickets_cp = [th.deepcopy() for th in thickets]
        inner_idx = thickets_cp[0].dataframe.index.names[1]
        # Step 0B: Pre-check of data structures
        _check_structures()

        # Step 1: Unify the thickets. Can be inplace since we are using copies already
        union_graph, _thickets = Ensemble._unify(
            thickets_cp, inplace=True, disable_tqdm=disable_tqdm
        )
        combined_th.graph = union_graph
        thickets_cp = _thickets

        # Step 2A: Handle performance data tables
        new_mappings = _handle_perfdata()
        # Step 2B: Handle metadata tables
        _handle_metadata()
        # Step 2C: Handle statistics table
        _handle_statsframe()
        # Step 2D: Handle other Thicket objects.
        _handle_misc()

        # Validate dataframe
        validate_dataframe(combined_th.dataframe)

        return combined_th

    @staticmethod
    def _index(
        thickets, from_statsframes=False, fill_perfdata=True, disable_tqdm=False
    ):
        """Unify a list of thickets into a single thicket

        Arguments:
            thickets (list): list of Thicket objects
            from_statsframes (bool): Whether this method was invoked from from_statsframes
            fill_perfdata (bool): whether to fill missing performance data with NaNs
            disable_tqdm (bool): whether to disable tqdm progress bar

        Returns:
            unify_graph (hatchet.Graph): unified graph,
            unify_df (DataFrame): unified dataframe,
            unify_exc_metrics (list): exclusive metrics,
            unify_inc_metrics (list): inclusive metrics,
            unify_metadata (DataFrame): unified metadata,
            unify_profile (list): profiles,
            unify_profile_mapping (dict): profile mapping
        """

        def _fill_perfdata(df, numerical_fill_value=np.nan):
            """Create full index for DataFrame and fill created rows with NaN's or None's where applicable.

            Arguments:
                df (DataFrame): DataFrame to fill missing rows in
                numerical_fill_value (any): value to fill numerical rows with

            Returns:
                (DataFrame): filled DataFrame
            """
            try:
                # Fill missing rows in dataframe with NaN's
                df = df.reindex(
                    pd.MultiIndex.from_product(df.index.levels),
                    fill_value=numerical_fill_value,
                )
                # Replace "NaN" with "None" in columns of string type
                for col in df.columns:
                    if pd.api.types.is_string_dtype(df[col].dtype):
                        df[col] = df[col].replace({numerical_fill_value: None})
            except ValueError as e:
                estr = str(e)
                if estr == "cannot handle a non-unique multi-index!":
                    warnings.warn(
                        "Non-unique multi-index for DataFrame in _fill_perfdata. Cannot Fill missing rows.",
                        RuntimeWarning,
                    )
                else:
                    raise

            return df

        # Add missing indicies to thickets
        helpers._resolve_missing_indicies(thickets)

        # Initialize attributes
        unify_graph = None
        unify_df = pd.DataFrame()
        unify_inc_metrics = []
        unify_exc_metrics = []
        unify_metadata = pd.DataFrame()
        unify_profile = []
        unify_profile_mapping = OrderedDict()

        # Unification
        unify_graph, thickets = Ensemble._unify(thickets, disable_tqdm=disable_tqdm)
        for th in thickets:
            # Extend metrics
            unify_inc_metrics.extend(th.inc_metrics)
            unify_exc_metrics.extend(th.exc_metrics)
            # Extend metadata
            if len(th.metadata) > 0:
                curr_meta = th.metadata.copy()
                unify_metadata = pd.concat([curr_meta, unify_metadata])
            # Extend profile
            if th.profile is not None:
                unify_profile.extend(th.profile)
            # Extend profile mapping
            if th.profile_mapping is not None:
                unify_profile_mapping.update(th.profile_mapping)
            # Extend dataframe
            unify_df = pd.concat([th.dataframe, unify_df])
        # Sort by keys
        unify_profile_mapping = OrderedDict(sorted(unify_profile_mapping.items()))

        # Validate unify_df before next operation
        validate_dataframe(unify_df)

        # Insert missing rows in dataframe
        if fill_perfdata:
            unify_df = _fill_perfdata(unify_df)

        # Sort PerfData
        unify_df.sort_index(inplace=True)
        # Sort Metadata
        unify_metadata.sort_index(inplace=True)

        # Remove duplicates in metrics
        unify_inc_metrics = list(set(unify_inc_metrics))
        unify_exc_metrics = list(set(unify_exc_metrics))

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
