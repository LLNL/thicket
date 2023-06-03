# Copyright 2022 Lawrence Livermore National Security, LLC and other
# Thicket Project Developers. See the top-level LICENSE file for details.
#
# SPDX-License-Identifier: MIT

import copy
import os
import json
import warnings
from collections import OrderedDict

import pandas as pd
import numpy as np
from hatchet import GraphFrame
from hatchet.query import AbstractQuery

import thicket.helpers as helpers
from .utils import verify_sorted_profile
from .utils import verify_thicket_structures


class Thicket(GraphFrame):
    """Ensemble of profiles, includes a graph and three dataframes, performance data,
    metadata, and aggregated statistics.
    """

    def __init__(
        self,
        graph,
        dataframe,
        exc_metrics=None,
        inc_metrics=None,
        default_metric="time",
        metadata={},
        performance_cols=None,
        profile=None,
        profile_mapping=None,
        statsframe=None,
    ):
        """Create a new thicket from a graph and a dataframe.

        Arguments:
            graph (Graph): graph of nodes in this thicket
            dataframe (DataFrame): pandas DataFrame indexed by Nodes from the graph, and
                potentially other indexes
            exc_metrics (list): list of names of exclusive metrics in the dataframe
            inc_metrics (list): list of names of inclusive metrics in the dataframe
            default_metric (str): primary metric
            metadata (DataFrame): pandas DataFrame indexed by profile hashes, contains
                profile metadata
            performance_cols (list): list of numeric columns within the performance
                dataframe
            profile (list): list of hashed profile strings
            profile_mapping (dict): mapping of hashed profile strings to original strings
            statsframe (DataFrame): pandas DataFrame indexed by Nodes from the graph
        """
        super().__init__(
            graph, dataframe, exc_metrics, inc_metrics, default_metric, metadata
        )
        self.profile = profile
        self.profile_mapping = profile_mapping
        if statsframe is None:
            self.statsframe = GraphFrame(
                graph=self.graph,
                dataframe=helpers._new_statsframe_df(dataframe),
            )
        else:
            self.statsframe = statsframe

        self.performance_cols = helpers._get_perf_columns(self.dataframe)

    def __eq__(self, other):
        """Compare two thicket objects.

        Arguments:
            other (Thicket): Thicket object to compare to

        Returns:
            (bool): True if equal, False otherwise
        """
        assert isinstance(other, Thicket)
        return (
            self.graph == other.graph
            and self.dataframe.equals(other.dataframe)
            and self.exc_metrics == other.exc_metrics
            and self.inc_metrics == other.inc_metrics
            and self.default_metric == other.default_metric
            and self.metadata.equals(other.metadata)
            and self.performance_cols == other.performance_cols
            and self.profile == other.profile
            and self.profile_mapping == other.profile_mapping
            and self.statsframe.graph == other.statsframe.graph
            and self.statsframe.dataframe.equals(other.statsframe.dataframe)
        )

    def __str__(self):
        s = (
            "graph: "
            + helpers._print_graph(self.graph)
            + "\ndataframe:\n"
            + self.dataframe
            + "\nexc_metrics: "
            + self.exc_metrics
            + "\ninc_metrics: "
            + self.inc_metrics
            + "\ndefault_metric: "
            + self.default_metric
            + "\nmetadata:\n"
            + self.metadata
            + "\nperformance_cols:\n"
            + self.performance_cols
            + "\nprofile: "
            + self.profile
            + "\nprofile_mapping: "
            + self.profile_mapping
            + "\nstatsframe:\n"
            + helpers._print_graph(self.statsframe.graph)
            + "\n"
            + self.statsframe.dataframe
        )

        return s

    @staticmethod
    def thicketize_graphframe(gf, prf):
        """Necessary function to handle output from using GraphFrame readers.

        Arguments:
            gf (GraphFrame): hatchet GraphFrame object
            prf (str): profile source of the GraphFrame

        Returns:
            (thicket): Thicket object
        """
        th = Thicket(
            graph=gf.graph,
            dataframe=gf.dataframe,
            exc_metrics=gf.exc_metrics,
            inc_metrics=gf.inc_metrics,
            metadata=gf.metadata,
        )
        if th.profile is None and isinstance(prf, str):
            # Store used profiles and profile mappings using a hash of their string
            hash_arg = hash(prf)
            th.profile = [hash_arg]
            th.profile_mapping = OrderedDict({hash_arg: [prf]})
            # format metadata as a dict of dicts
            temp_meta = {}
            temp_meta[hash_arg] = th.metadata
            th.metadata = pd.DataFrame.from_dict(temp_meta, orient="index")
            th.metadata.index.set_names("profile", inplace=True)

            # Add profile to dataframe index
            th.dataframe["profile"] = hash_arg
            index_names = list(th.dataframe.index.names)
            index_names.insert(1, "profile")
            th.dataframe.reset_index(inplace=True)
            th.dataframe.set_index(index_names, inplace=True)
        return th

    @staticmethod
    def from_caliper(filename_or_stream, query=None, intersection=False):
        """Read in a Caliper .cali or .json file.

        Arguments:
            filename_or_stream (str or file-like): name of a Caliper output file in
                `.cali` or JSON-split format, or an open file object to read one
            query (str): cali-query in CalQL format
            intersection (bool): whether to perform intersection or union (default)
        """
        return Thicket.reader_dispatch(
            GraphFrame.from_caliper, intersection, filename_or_stream, query
        )

    @staticmethod
    def from_hpctoolkit(dirname, intersection=False):
        """Create a GraphFrame using hatchet's HPCToolkit reader and use its attributes
        to make a new thicket.

        Arguments:
            dirname (str): parent directory of an HPCToolkit experiment.xml file
            intersection (bool): whether to perform intersection or union (default)

        Returns:
            (thicket): new thicket containing HPCToolkit profile data
        """
        return Thicket.reader_dispatch(
            GraphFrame.from_hpctoolkit, intersection, dirname
        )

    @staticmethod
    def from_caliperreader(filename_or_caliperreader, intersection=False):
        """Helper function to read one caliper file.

        Arguments:
            filename_or_caliperreader (str or CaliperReader): name of a Caliper output
                file in `.cali` format, or a CaliperReader object
            intersection (bool): whether to perform intersection or union (default)
        """
        return Thicket.reader_dispatch(
            GraphFrame.from_caliperreader, intersection, filename_or_caliperreader
        )

    @staticmethod
    def reader_dispatch(func, intersection=False, *args, **kwargs):
        """Create a thicket from a list, directory of files, or a single file.

        Arguments:
            func (function): reader function to be used
            intersection (bool): whether to perform intersection or union (default).
            args (list): list of args; args[0] should be an object that can be read from
        """
        ens_list = []
        obj = args[0]  # First arg should be readable object

        # Parse the input object
        # if a list of files
        if type(obj) == list:
            for file in obj:
                ens_list.append(Thicket.thicketize_graphframe(func(file), file))
        # if directory of files
        elif os.path.isdir(obj):
            for file in os.listdir(obj):
                f = os.path.join(obj, file)
                ens_list.append(Thicket.thicketize_graphframe(func(f), f))
        # if single file
        elif os.path.isfile(obj):
            return Thicket.thicketize_graphframe(func(*args, **kwargs), args[0])
        else:
            raise TypeError(type(obj) + " is not a valid type to be read from.")

        # Perform unify ensemble
        thicket_object = Thicket.unify_ensemble(ens_list)
        if intersection:
            thicket_object.intersection()
        return thicket_object

    @staticmethod
    def from_json(json_thicket):
        # deserialize the json
        thicket_dict = json.loads(json_thicket)
        hatchet_spec = {
            "graph": thicket_dict["graph"],
            "dataframe": thicket_dict["dataframe"],
            "dataframe_indices": thicket_dict["dataframe_indices"],
            "exclusive_metrics": thicket_dict["inclusive_metrics"],
            "inclusive_metrics": thicket_dict["exclusive_metrics"],
        }

        gf = GraphFrame.from_json(json.dumps(hatchet_spec))

        th = Thicket(
            graph=gf.graph,
            dataframe=gf.dataframe,
            exc_metrics=thicket_dict["exclusive_metrics"],
            inc_metrics=thicket_dict["inclusive_metrics"],
            profile=thicket_dict["profile"],
            profile_mapping=thicket_dict["profile_mapping"],
        )

        if "metadata" in thicket_dict:
            mf = pd.DataFrame(thicket_dict["metadata"])
            mf.set_index(mf["profile"], inplace=True)
            if "profile" in mf.columns:
                mf = mf.drop(columns=["profile"])
            th.metadata = mf

        # catch condition where there are no stats
        if "stats" in thicket_dict:
            sf_spec = {
                "graph": thicket_dict["graph"],
                "dataframe": thicket_dict["stats"],
                "dataframe_indices": ["node"],
                "exclusive_metrics": [],
                "inclusive_metrics": [],
            }
            th.statsframe = GraphFrame.from_json(json.dumps(sf_spec))
            th.statsframe.graph = th.graph

        # make and return thicket?
        return th

    @staticmethod
    def columnar_join(
        thicket_list,
        header_list=None,
        column_name=None,
    ):
        """Join Thickets column-wise. New column multi-index will be created with
        columns under separate indexer headers.

        Arguments:
            thicket_list (list): List of Thickets to join
            header_list (list): List of headers to use for the new columnar multi-index
            column_name (str): Name of the column from the metadata table to join on. If
                no argument is provided, it is assumed that there is no profile-wise
                relationship between self and other.

        Returns:
            (Thicket): New Thicket object with joined columns
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
        for th in thicket_list:
            verify_thicket_structures(th.dataframe, index=["node", "profile"])
            verify_thicket_structures(th.statsframe.dataframe, index=["node"])
            verify_thicket_structures(th.metadata, index=["profile"])
        # Check for column_name in metadata
        if column_name:
            for th in thicket_list:
                verify_thicket_structures(th.metadata, columns=[column_name])
        # Check length of profiles match
        for i in range(len(thicket_list) - 1):
            if len(thicket_list[i].profile) != len(thicket_list[i + 1].profile):
                raise ValueError(
                    "Length of all thicket profiles must match. {} != {}".format(
                        len(thicket_list[i].profile), len(thicket_list[i + 1].profile)
                    )
                )
        # Ensure all thickets profiles are sorted. Must be true when column_name=None to
        # guarantee performance data table and metadata table match up.
        if column_name is None:
            for th in thicket_list:
                verify_sorted_profile(th.dataframe)
                verify_sorted_profile(th.metadata)

        ###
        # Step 0B: Variable Initialization
        ###
        # Initialize combined thicket
        combined_th = thicket_list[0].deepcopy()
        # Use copies to be non-destructive
        thicket_list_cp = [th.deepcopy() for th in thicket_list]

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
            header_list = [i for i in range(len(thicket_list))]

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

    def add_column_from_metadata_to_ensemble(
        self, column_name, overwrite=False, drop=False
    ):
        """Add a column from the metadata table to the performance data table.

        Arguments:
            column_name (str): Name of the column from the metadata table
            overwrite (bool): Determines overriding behavior in performance data table
            drop (bool): Whether to drop the column from the metadata table afterwards
        """
        # Add warning if column already exists in performance data table
        if column_name in self.dataframe.columns:
            # Drop column to overwrite, otherwise warn and return
            if overwrite:
                self.dataframe.drop(column_name, axis=1, inplace=True)
            else:
                warnings.warn(
                    "Column "
                    + column_name
                    + " already exists. Set 'overwrite=True' to force update the column."
                )
                return

        # Add the column to the performance data table
        self.dataframe = self.dataframe.join(
            self.metadata[column_name], on=self.dataframe.index.names[1]
        )

        # Drop column
        if drop:
            self.metadata.drop(column_name, axis=1, inplace=True)

    def squash(self, update_inc_cols=True):
        """Rewrite the Graph to include only nodes present in the performance
        data table's rows.

        This can be used to simplify the Graph, or to normalize Graph indexes between
        two Thickets.

        Arguments:
            update_inc_cols (boolean, optional): if True, update inclusive columns.

        Returns:
            (Thicket): a newly squashed Thicket object
        """
        squashed_gf = GraphFrame.squash(self, update_inc_cols=update_inc_cols)
        new_graph = squashed_gf.graph
        # The following code updates the performance data and the aggregated statistics
        # table with the remaining (re-indexed) nodes. The dataframe is internally
        # updated in squash(), so we can easily just save it to our thicket performance
        # data. For the aggregated statistics table, we'll have to come up with a better
        # way eventually, but for now, we'll just create a new aggregated statistics
        # table the same way we do when we create a new thicket.
        new_dataframe = squashed_gf.dataframe
        stats_df = helpers._new_statsframe_df(new_dataframe)
        sframe = GraphFrame(
            graph=new_graph,
            dataframe=stats_df,
        )
        return Thicket(
            new_graph,
            new_dataframe,
            exc_metrics=self.exc_metrics,
            inc_metrics=self.inc_metrics,
            default_metric=self.default_metric,
            metadata=self.metadata,
            profile=self.profile,
            profile_mapping=self.profile_mapping,
            statsframe=sframe,
        )

    def copy(self):
        """Return a partially shallow copy of the Thicket.

        See GraphFrame.copy() for more details

        Arguments:
            self (Thicket): object to make a copy of

        Returns:
            other (Thicket): copy of self
                (graph ... default_metric): Same behavior as GraphFrame
                metadata (DataFrame): pandas "non-deep" copy of dataframe
                profile (list): copy of self's profile
                profile_mapping (dict): copy of self's profile_mapping
                statsframe (GraphFrame): calls GraphFrame.copy()
        """
        gf = GraphFrame.copy(self)

        return Thicket(
            graph=gf.graph,
            dataframe=gf.dataframe,
            exc_metrics=gf.exc_metrics,
            inc_metrics=gf.inc_metrics,
            default_metric=gf.default_metric,
            metadata=self.metadata.copy(deep=False),
            profile=copy.copy(self.profile),
            profile_mapping=copy.copy(self.profile_mapping),
            statsframe=self.statsframe.copy(),
        )

    def deepcopy(self):
        """Return a deep copy of the Thicket.

        See GraphFrame.deepcopy() for more details

        Arguments:
            self (Thicket): object to make a copy of

        Returns:
            other (Thicket): copy of self
                (graph ... default_metric): same behavior as GraphFrame
                metadata (DataFrame): pandas "deep" copy of dataframe
                profile (list): copy of self's profile
                profile_mapping (dict): copy of self's profile_mapping
                statsframe (GraphFrame): Calls GraphFrame.deepcopy()
        """
        gf = GraphFrame.deepcopy(self)

        return Thicket(
            graph=gf.graph,
            dataframe=gf.dataframe,
            exc_metrics=gf.exc_metrics,
            inc_metrics=gf.inc_metrics,
            default_metric=gf.default_metric,
            metadata=self.metadata.copy(),
            profile=copy.deepcopy(self.profile),
            profile_mapping=copy.deepcopy(self.profile_mapping),
            statsframe=self.statsframe.deepcopy(),
        )

    def tree(self):
        """hatchet tree() function for a thicket"""
        temp_df = self.statsframe.dataframe.copy()
        # Adjustments specific for multi-index.
        if isinstance(temp_df.columns, pd.MultiIndex):
            temp_df.columns = temp_df.columns.to_flat_index()
            temp_df.rename(columns={("", "name"): "name"}, inplace=True)
        # Placeholder value. TODO: Enable selection from performance data table.
        temp_df["thicket_tree"] = -1
        return GraphFrame.tree(
            self=Thicket(graph=self.graph, dataframe=temp_df),
            metric_column="thicket_tree",
        )

    def unify_pair(self, other):
        """Unify two Thicket's graphs and dataframes"""
        # Check for the same object. Cheap operation since no graph walkthrough.
        if self.graph is other.graph:
            print("same graph (object)")
            return self.graph

        # Check for the same graph structure. Need to walk through graphs *but should
        # still be less expensive then performing the rest of this function.*
        if self.graph == other.graph:
            print("same graph (structure)")
            return self.graph

        print("different graph")

        node_map = {}
        union_graph = self.graph.union(other.graph, node_map)

        self_index_names = self.dataframe.index.names
        other_index_names = other.dataframe.index.names

        self.dataframe.reset_index(inplace=True)
        other.dataframe.reset_index(inplace=True)

        self.dataframe["node"] = self.dataframe["node"].apply(lambda x: node_map[id(x)])
        other.dataframe["node"] = other.dataframe["node"].apply(
            lambda x: node_map[id(x)]
        )

        self.dataframe.set_index(self_index_names, inplace=True)
        other.dataframe.set_index(other_index_names, inplace=True)

        self.graph = union_graph
        other.graph = union_graph

        return union_graph

    @staticmethod
    def unify_pairwise(th_list, debug=False):
        """Unifies two thickets graphs and dataframes.

        Ensure self and other have the same graph and same node IDs. This may change the
        node IDs in the dataframe.

        Update the graphs in the graphframe if they differ.

        Arguments:
            th_list (list): list of Thicket objects
            debug (bool): print debug statements

        Returns:
            union_graph (Graph): unified graph
        """
        union_graph = th_list[0].graph
        for i in range(len(th_list)):
            for j in range(i + 1, len(th_list)):
                if debug:
                    print("Unifying (" + str(i) + ", " + str(j) + "...")
                union_graph = th_list[i].unify_pair(th_list[j])
        return union_graph

    @staticmethod
    def unify_listwise(th_list, debug=False):
        """Unify a list of Thicket's graphs and dataframes

        Arguments:
            th_list (list): list of Thicket objects
            debug (bool): print debug statements

        Returns:
            union_graph (Graph): unified graph
        """
        # variable to keep track of case where all graphs are the same
        same_graphs = True

        # GRAPH UNIFICATION
        union_graph = th_list[0].graph
        for i in range(1, len(th_list)):  # n-1 unions
            # Check to skip unnecessary computation. apply short circuiting with 'or'.
            if union_graph is th_list[i].graph or union_graph == th_list[i].graph:
                if debug:
                    print("Union Graph == thicket[" + str(i) + "].graph")
            else:
                if debug:
                    print("Unifying (Union Graph, " + str(i) + ")")
                same_graphs = False
                # Unify graph with current thickets graph
                union_graph = union_graph.union(th_list[i].graph)

        # If the graphs were all the same in the first place then there is no need to
        # apply any node mappings.
        if same_graphs:
            return union_graph

        # DATAFRAME MAPPING UPDATE
        for i in range(len(th_list)):  # n ops
            node_map = {}
            # Create a node map from current thickets graph to the union graph. This is
            # only valid once the union graph is complete.
            union_graph.union(th_list[i].graph, node_map)
            names = th_list[i].dataframe.index.names
            th_list[i].dataframe.reset_index(inplace=True)

            # Apply node_map mapping
            th_list[i].dataframe["node"] = (
                th_list[i].dataframe["node"].apply(lambda node: node_map[id(node)])
            )
            th_list[i].dataframe.set_index(names, inplace=True, drop=True)

        # After this point the graph and dataframe in each thicket is out of sync.
        # We could update the graph element in thicket to be the union graph but if the
        # user prints out the graph how do we annotate nodes only contained in one
        # thicket.
        return union_graph

    @staticmethod
    def unify_ensemble(th_list, pairwise=False, superthicket=False):
        """Unify a list of thickets into a single thicket

        Arguments:
            th_list (list): list of thickets
            pairwise (bool): use the pairwise implementation of unify (use if having
                issues)
            superthicket (bool): whether the result is a superthicket

        Returns:
            (thicket): unified thicket
        """
        unify_graph = None
        if pairwise:
            unify_graph = Thicket.unify_pairwise(th_list)
        else:
            unify_graph = Thicket.unify_listwise(th_list)

        helpers._resolve_missing_indicies(th_list)

        # Unify dataframe
        unify_df = pd.DataFrame()
        unify_inc_metrics = []
        unify_exc_metrics = []
        unify_metadata = pd.DataFrame()
        unify_profile = []
        unify_profile_mapping = {}

        # Unification loop
        for i, th in enumerate(th_list):
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

        unify_th = Thicket(
            graph=unify_graph,
            dataframe=unify_df,
            exc_metrics=unify_exc_metrics,
            inc_metrics=unify_inc_metrics,
            metadata=unify_metadata,
            profile=unify_profile,
            profile_mapping=unify_profile_mapping,
        )
        return unify_th

    @staticmethod
    def make_superthicket(th_list, profiles_from_meta=None):
        """Convert a list of thickets into a 'superthicket'.

        Their individual aggregated statistics table are ensembled and become the
        superthicket's performance data table.

        Arguments:
            th_list (list): list of thickets
            profiles_from_meta (str, optional): name of the metadata column to use as
                the new second-level index. Uses the first value so this only makes
                sense if provided column is all equal values and each thicket's columns
                differ in value.

        Returns:
            (thicket): superthicket
        """
        # Pre-check of data structures
        for th in th_list:
            verify_thicket_structures(
                th.dataframe, index=["node", "profile"]
            )  # Required for deepcopy operation
            verify_thicket_structures(
                th.statsframe.dataframe, index=["node"]
            )  # Required for deepcopy operation

        # Setup names list
        th_names = []
        if profiles_from_meta is None:
            for i in range(len(th_list)):
                th_names.append(i)
        else:  # profiles_from_meta was provided.
            for th in th_list:
                # Get name from metadata table
                name_list = th.metadata[profiles_from_meta].tolist()

                if len(name_list) > 1:
                    warnings.warn(
                        f"Multiple values for name {name_list} at thicket.metadata[{profiles_from_meta}]. Only the first will be used."
                    )
                th_names.append(name_list[0])

        th_copy_list = []
        for i in range(len(th_list)):
            th_copy = th_list[i].deepcopy()

            th_id = th_names[i]

            # Modify graph
            # Necessary so node ids match up
            th_copy.graph = th_copy.statsframe.graph

            # Modify the performance data table
            df = th_copy.statsframe.dataframe
            df["thicket"] = th_id
            df.set_index("thicket", inplace=True, append=True)
            th_copy.dataframe = df

            # Adjust profile and profile_mapping
            th_copy.profile = [th_id]
            profile_paths = list(th_copy.profile_mapping.values())
            th_copy.profile_mapping = OrderedDict({th_id: profile_paths})

            # Modify metadata dataframe
            idx_name = "new_idx"
            th_copy.metadata[idx_name] = th_id
            th_copy.metadata.set_index(idx_name, inplace=True)

            # Append copy to list
            th_copy_list.append(th_copy)

        return Thicket.unify_ensemble(th_copy_list, superthicket=True)

    def to_json(self, ensemble=True, metadata=True, stats=True):
        jsonified_thicket = {}

        # jsonify graph
        """
        Nodes: {hatchet_nid: {node data, children:[by-id]}}
        """
        formatted_graph_dict = {}

        for n in self.graph.traverse():
            formatted_graph_dict[n._hatchet_nid] = {
                "data": n.frame.attrs,
                "children": [c._hatchet_nid for c in n.children],
                "parents": [c._hatchet_nid for c in n.parents],
            }

        jsonified_thicket["graph"] = [formatted_graph_dict]

        if ensemble:
            jsonified_thicket["dataframe_indices"] = list(self.dataframe.index.names)
            ef = self.dataframe.reset_index()
            ef["node"] = ef["node"].apply(lambda n: n._hatchet_nid)
            jsonified_thicket["dataframe"] = ef.replace({np.nan: None}).to_dict(
                "records"
            )
        if metadata:
            jsonified_thicket["metadata"] = (
                self.metadata.reset_index().replace({np.nan: None}).to_dict("records")
            )
        if stats:
            sf = self.statsframe.dataframe.copy(deep=True)
            sf.index = sf.index.map(lambda n: n._hatchet_nid)
            jsonified_thicket["stats"] = (
                sf.replace({np.nan: None}).reset_index().to_dict("records")
            )

        jsonified_thicket["inclusive_metrics"] = self.inc_metrics
        jsonified_thicket["exclusive_metrics"] = self.exc_metrics
        jsonified_thicket["profile"] = self.profile
        jsonified_thicket["profile_mapping"] = self.profile_mapping

        return json.dumps(jsonified_thicket)

    def intersection(self):
        """Perform an intersection operation on a thicket.

        Nodes not contained in all profiles are removed.

        Returns:
            remaining_node_list (list): list of nodes that were not removed
            removed_node_list (list): list of removed nodes
        """
        # Filter the performance data table
        total_profiles = len(self.profile)
        remaining_node_list = []  # Needed for graph
        removed_node_list = []

        # For each node
        for node, new_df in self.dataframe.groupby(level=0):
            # Use profile count to make decision
            if len(new_df) < total_profiles:
                removed_node_list.append(node)
            else:
                remaining_node_list.append(node)
        self.dataframe.drop(removed_node_list, inplace=True)

        # Propagate change to aggregated statistics table
        self.statsframe.dataframe.drop(removed_node_list, inplace=True)

        # Filter the graph

        # Remove roots
        self.graph.roots = list(set(self.graph.roots).intersection(remaining_node_list))
        for node in self.graph.traverse():
            # Remove children and parents that DNE in the intersection
            new_children = []
            new_parents = []
            for child in node.children:
                if child in remaining_node_list:
                    new_children.append(child)
            for parent in node.parents:
                if parent in remaining_node_list:
                    new_parents.append(parent)
            node.children = new_children
            node.parents = new_parents

        # Update hatchet nids
        self.graph.enumerate_traverse()

        return remaining_node_list, removed_node_list

    def filter_metadata(self, select_function):
        """Filter thicket object based on a metadata key.

        Changes are propagated to the entire thicket object.

        Arguments:
            select_function (lambda function): filter to apply to the metadata table

        Returns:
            (thicket): new thicket object with selected value
        """
        if callable(select_function):
            if not self.metadata.empty:
                # check only 1 index in metadata
                assert self.metadata.index.nlevels == 1

                # Add warning if filtering on multi-index columns
                if isinstance(self.metadata.columns, pd.MultiIndex):
                    warnings.warn(
                        "Filtering on MultiIndex columns will impact the entire row, not just the subsection of the provided MultiIndex."
                    )

                # Get index name
                index_name = self.metadata.index.name

                # create a copy of the thicket object
                new_thicket = self.copy()

                # filter metadata table
                filtered_rows = new_thicket.metadata.apply(select_function, axis=1)
                new_thicket.metadata = new_thicket.metadata[filtered_rows]

                # note index keys to filter performance data table
                index_id = new_thicket.metadata.index.values.tolist()
                # filter performance data table based on the metadata table
                new_thicket.dataframe = new_thicket.dataframe[
                    new_thicket.dataframe.index.get_level_values(index_name).isin(
                        index_id
                    )
                ]

                # create an empty aggregated statistics table with the name column
                new_thicket.statsframe.dataframe = helpers._new_statsframe_df(
                    new_thicket.dataframe
                )
            else:
                raise EmptyMetadataTable(
                    "The provided Thicket object has an empty MetadataTable."
                )

        else:
            raise InvalidFilter("The argument passed to filter must be a callable.")

        return new_thicket

    def filter(self, filter_func):
        """Overloaded generic filter function.

        Provides a helper message for using the thicket filter functions.
        """
        raise RuntimeError(
            "Invalid function: thicket.filter(), please use thicket.filter_metadata() or thicket.filter_stats()"
        )

    def query(self, query_obj, squash=True, update_inc_cols=True):
        """Apply a Hatchet query to the Thicket object.

        Arguments:
            query_obj (AbstractQuery): the query, represented as by a subclass of
                Hatchet's AbstractQuery
            squash (bool): if true, run Thicket.squash before returning the result of
                the query
            update_inc_cols (boolean, optional): if True, update inclusive columns when
                performing squash.

        Returns:
            (Thicket): a new Thicket object containing the data that matches the query
        """
        if isinstance(query_obj, (list, str)):
            raise UnsupportedQuery(
                "Object and String queries from Hatchet are not yet supported in Thicket"
            )
        elif not issubclass(type(query_obj), AbstractQuery):
            raise TypeError(
                "Input to 'query' must be a Hatchet query (i.e., list, str, or subclass of AbstractQuery)"
            )
        dframe_copy = self.dataframe.copy()
        index_names = self.dataframe.index.names
        dframe_copy.reset_index(inplace=True)
        query = query_obj
        # TODO Add a conditional here to parse Object and String queries when supported
        query_matches = query.apply(self)
        filtered_df = dframe_copy.loc[dframe_copy["node"].isin(query_matches)]
        if filtered_df.shape[0] == 0:
            raise EmptyQuery("The provided query would have produced an empty Thicket.")
        filtered_df.set_index(index_names, inplace=True)
        filtered_th = Thicket(
            self.graph,
            filtered_df,
            exc_metrics=self.exc_metrics,
            inc_metrics=self.inc_metrics,
            default_metric=self.default_metric,
            metadata=self.metadata,
            profile=self.profile,
            profile_mapping=self.profile_mapping,
            statsframe=self.statsframe,
        )
        if squash:
            return filtered_th.squash(update_inc_cols=update_inc_cols)
        return filtered_th

    def groupby(self, groupby_function):
        """Create sub-thickets based on unique values in metadata column(s).

        Arguments:
            groupby_function (groupby_function): groupby function on dataframe

        Returns:
            (list): list of (sub)thickets
        """
        if not self.metadata.empty:
            # group metadata table by unique values in a column
            sub_metadataframes = self.metadata.groupby(groupby_function, dropna=False)

            list_sub_thickets = []
            unique_vals = []

            # for all unique groups of metadata table
            for key, df in sub_metadataframes:
                unique_vals.append(key)

                # create a thicket copy
                sub_thicket = self.copy()

                # return unique group as the metadata table
                sub_thicket.metadata = df

                # find profiles in current unique group and filter performance data
                # table
                profile_id = df.index.values.tolist()
                sub_thicket.dataframe = sub_thicket.dataframe[
                    sub_thicket.dataframe.index.get_level_values("profile").isin(
                        profile_id
                    )
                ]

                # Updates the profiles to only contain the remaining ones
                profile_mapping_tmp = sub_thicket.profile_mapping.copy()
                for profile_mapping_key in profile_mapping_tmp:
                    if profile_mapping_key not in profile_id:
                        sub_thicket.profile_mapping.pop(profile_mapping_key)

                sub_thicket.profile = [
                    profile for profile in sub_thicket.profile if profile in profile_id
                ]

                # clear the aggregated statistics table for current unique group
                sub_thicket.statsframe.dataframe = helpers._new_statsframe_df(
                    sub_thicket.dataframe
                )
                list_sub_thickets.append(sub_thicket)
        else:
            raise EmptyMetadataTable(
                "The provided Thicket object has an empty metadata table."
            )

        print(len(list_sub_thickets), " thickets created...")
        print(unique_vals)

        return list_sub_thickets

    def filter_stats(self, filter_function):
        """Filter thicket object based on a stats column.

        Propagate changes to the entire thicket object.

        Arguments:
            select_function (lambda function): filter to apply to the aggregated
                statistics table

        Returns:
            (thicket): new thicket object with applied filter function
        """
        # copy thicket
        new_thicket = self.copy()

        # filter aggregated statistics table based on greater than restriction
        filtered_rows = new_thicket.statsframe.dataframe.apply(filter_function, axis=1)
        new_thicket.statsframe.dataframe = new_thicket.statsframe.dataframe[
            filtered_rows
        ]

        # filter performance data table based on filtered nodes
        filtered_nodes = new_thicket.statsframe.dataframe.index.values.tolist()
        new_thicket.dataframe = new_thicket.dataframe[
            new_thicket.dataframe.index.get_level_values("node").isin(filtered_nodes)
        ]

        # filter nodes in the graphframe based on the dataframe nodes
        # TODO see if the new Thicket.squash function will work here
        filtered_graphframe = GraphFrame.squash(new_thicket)
        new_thicket.graph = filtered_graphframe.graph
        new_thicket.statsframe.graph = filtered_graphframe.graph

        return new_thicket

    def get_unique_metadata(self):
        """Get unique values per column in metadata.

        Returns:
            (dict): alphabetical ordered dictionary with key's being the column names
                and the values being unique values for a metadata column.
        """
        unique_meta = {}

        # thicket object without columnar index
        if self.dataframe.columns.nlevels == 1:
            for col in self.metadata.columns:
                # skip columns where the values are a list
                if isinstance(self.metadata[col].iloc[0], list):
                    continue
                else:
                    unique_entries = self.metadata[col].unique().tolist()
                    unique_meta[col] = unique_entries

            sorted_meta = dict(sorted(unique_meta.items(), key=lambda x: x[0].lower()))
        # columnar joined thicket object
        else:
            sorted_meta = []
            for idx in list(self.metadata.columns.levels[0]):
                for col in self.metadata[idx].columns:
                    if isinstance(self.metadata[idx][col].iloc[0], list):
                        continue
                    else:
                        unique_entries = self.metadata[idx][col].unique().tolist()
                        unique_meta[col] = unique_entries

                sorted_meta.append(
                    (idx, dict(sorted(unique_meta.items(), key=lambda x: x[0].lower())))
                )

        return sorted_meta


class InvalidFilter(Exception):
    """Raised when an invalid argument is passed to the filter function."""


class EmptyMetadataTable(Exception):
    """Raised when a Thicket object argument is passed with an empty MetadataTable to
    the filter function.
    """


class UnsupportedQuery(Exception):
    """Raised when an object query or string query are provided to the 'query' function
    because those types of queries are not yet supported in Thicket.
    """


class EmptyQuery(Exception):
    """Raised when a query would result in an empty Thicket object."""
