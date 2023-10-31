# Copyright 2022 Lawrence Livermore National Security, LLC and other
# Thicket Project Developers. See the top-level LICENSE file for details.
#
# SPDX-License-Identifier: MIT

from collections import OrderedDict

from hatchet import GraphFrame
import numpy as np
import pandas as pd

import thicket.helpers as helpers
from .utils import validate_dataframe, verify_sorted_profile, verify_thicket_structures


class Ensemble:
    """Operations pertaining to ensembling."""

    @staticmethod
    def _unify(thickets, inplace=False):
        """Create union graph from list of thickets and sync their DataFrames.

        Arguments:
            thickets (list): list of Thicket objects
            inplace (bool): whether to modify the original thicket objects or return new

        Returns:
            (tuple): tuple containing:
                (hatchet.Graph): unified graph
                (list): list of Thicket objects
        """
        _thickets = thickets
        if not inplace:
            _thickets = [th.deepcopy() for th in thickets]
        # Unify graphs if "self" and "other" do not have the same graph
        union_graph = _thickets[0].graph
        for i in range(len(_thickets) - 1):
            # Check for same graph id (fast) if not -> check for equality (slow)
            if (
                _thickets[i].graph is _thickets[i + 1].graph
                or _thickets[i].graph == _thickets[i + 1].graph
            ):
                continue
            else:
                union_graph = union_graph.union(_thickets[i + 1].graph)
        for i in range(len(_thickets)):
            # Set all graphs to the union graph
            _thickets[i].graph = union_graph
            # For tree diff. dataframes need to be sorted.
            _thickets[i].dataframe.sort_index(inplace=True)
            # Necessary to change dataframe hatchet id's to match the nodes in the graph
            helpers._sync_nodes_frame(union_graph, _thickets[i].dataframe)
        return union_graph, _thickets

    @staticmethod
    def _columns(
        thickets,
        headers=None,
        metadata_key=None,
    ):
        """Concatenate Thicket attributes horizontally. For DataFrames, this implies expanding
        in the column direction. New column multi-index will be created with columns
        under separate indexer headers.

        Arguments:
            headers (list): List of headers to use for the new columnar multi-index
            metadata_key (str): Name of the column from the metadata tables to replace the 'profile'
                index. If no argument is provided, it is assumed that there is no profile-wise
                relationship between the thickets.

        Returns:
            (Thicket): New ensembled Thicket object
        """

        def _check_structures():
            """Check that the structures of the thicket objects are valid for the incoming operations."""
            # Required/expected format of the data
            for th in thickets:
                verify_thicket_structures(th.dataframe, index=["node", "profile"])
                verify_thicket_structures(th.statsframe.dataframe, index=["node"])
                verify_thicket_structures(th.metadata, index=["profile"])
            # Check for metadata_key in metadata
            if metadata_key:
                for th in thickets:
                    verify_thicket_structures(th.metadata, columns=[metadata_key])
            # Check length of profiles match if metadata key is not provided
            if metadata_key is None:
                for i in range(len(thickets) - 1):
                    if len(thickets[i].profile) != len(thickets[i + 1].profile):
                        raise ValueError(
                            "Length of all thicket profiles must match if 'metadata_key' is not provided. {} != {}".format(
                                len(thickets[i].profile), len(thickets[i + 1].profile)
                            )
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
            for i in range(len(thickets_cp)):
                thickets_cp[i].metadata.reset_index(drop=True, inplace=True)
            if metadata_key is None:
                for i in range(len(thickets_cp)):
                    thickets_cp[i].metadata.index.set_names("profile", inplace=True)
            else:
                for i in range(len(thickets_cp)):
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
                    thickets_cp[i].dataframe.reset_index(level="profile", inplace=True)
                    new_mappings.update(
                        pd.Series(
                            thickets_cp[i]
                            .dataframe["new_profiles"]
                            .map(lambda x: (x, headers[i]))
                            .values,
                            index=thickets_cp[i].dataframe["profile"],
                        ).to_dict()
                    )
                    thickets_cp[i].dataframe.drop("profile", axis=1, inplace=True)
                    thickets_cp[i].dataframe.set_index(
                        "new_profiles", append=True, inplace=True
                    )
                    thickets_cp[i].dataframe.index.rename(
                        "profile", level="new_profiles", inplace=True
                    )
            else:  # Change second-level index to be from metadata's "metadata_key" column
                for i in range(len(thickets_cp)):
                    thickets_cp[i].metadata_column_to_perfdata(metadata_key)
                    thickets_cp[i].dataframe.reset_index(level="profile", inplace=True)
                    new_mappings.update(
                        pd.Series(
                            thickets_cp[i]
                            .dataframe[metadata_key]
                            .map(lambda x: (x, headers[i]))
                            .values,
                            index=thickets_cp[i].dataframe["profile"],
                        ).to_dict()
                    )
                    thickets_cp[i].dataframe.drop("profile", axis=1, inplace=True)
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

        # Step 0A: Pre-check of data structures
        _check_structures()
        # Step 0B: Variable Initialization
        combined_th = thickets[0].deepcopy()
        thickets_cp = [th.deepcopy() for th in thickets]

        # Step 1: Unify the thickets
        union_graph, _thickets = Ensemble._unify(thickets_cp)
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
    def _index(thickets, from_statsframes=False):
        """Unify a list of thickets into a single thicket

        Arguments:
            from_statsframes (bool): Whether this method was invoked from from_statsframes

        Returns:
            unify_graph (hatchet.Graph): unified graph,
            unify_df (DataFrame): unified dataframe,
            unify_exc_metrics (list): exclusive metrics,
            unify_inc_metrics (list): inclusive metrics,
            unify_metadata (DataFrame): unified metadata,
            unify_profile (list): profiles,
            unify_profile_mapping (dict): profile mapping
        """

        def _fill_perfdata(perfdata, fill_value=np.nan):
            # Fill missing rows in dataframe with NaN's
            perfdata = perfdata.reindex(
                pd.MultiIndex.from_product(perfdata.index.levels), fill_value=fill_value
            )
            # Replace "NaN" with "None" in columns of string type
            for col in perfdata.columns:
                if pd.api.types.is_string_dtype(perfdata[col].dtype):
                    perfdata[col].replace({fill_value: None}, inplace=True)

            return perfdata

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
        unify_graph, thickets = Ensemble._unify(thickets)
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

        # Insert missing rows in dataframe
        unify_df = _fill_perfdata(unify_df)

        # Sort PerfData
        unify_df.sort_index(inplace=True)
        # Sort Metadata
        unify_metadata.sort_index(inplace=True)

        # Remove duplicates in metrics
        unify_inc_metrics = list(set(unify_inc_metrics))
        unify_exc_metrics = list(set(unify_exc_metrics))

        # Validate unify_df
        validate_dataframe(unify_df)

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
