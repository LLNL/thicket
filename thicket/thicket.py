# Copyright 2022 Lawrence Livermore National Security, LLC and other
# Thicket Project Developers. See the top-level LICENSE file for details.
#
# SPDX-License-Identifier: MIT

import copy
import os
import json
import warnings

import pandas as pd
import numpy as np

from collections import OrderedDict
from hatchet import GraphFrame
from .helpers import print_graph


class Thicket(GraphFrame):
    """Ensemble of profiles"""

    def __init__(
        self,
        graph,
        dataframe,
        exc_metrics=None,
        inc_metrics=None,
        default_metric="time",
        metadata={},
        profile=None,
        profile_mapping=None,
        statsframe=None,
    ):
        """Create a new thicket from a graph and a dataframe.

        Arguments:
            graph (Graph): Graph of nodes in this thicket.
            dataframe (DataFrame): Pandas DataFrame indexed by Nodes from the graph, and potentially other indexes.
            exc_metrics: list of names of exclusive metrics in the dataframe.
            inc_metrics: list of names of inclusive metrics in the dataframe.
            metadata (DataFrame): Pandas DataFrame indexed by profile hashes, contains profile metadata.
            profile (list): List of hashed profile strings.
            profile_mapping (dict): Mapping of hashed profile strings to original strings.
        """
        super().__init__(
            graph,
            dataframe,
            exc_metrics,
            inc_metrics,
            default_metric,
            metadata,
        )
        self.profile = profile
        self.profile_mapping = profile_mapping
        if statsframe is None:
            subset_df = (
                dataframe["name"].reset_index().drop_duplicates(subset=["node"])
            )  # Drop duplicates based on nid
            self.statsframe = GraphFrame(
                graph=self.graph,
                dataframe=pd.DataFrame(
                    index=subset_df["node"],
                    data={"name": subset_df["name"].values},
                ),
            )
        else:
            self.statsframe = statsframe

    def __str__(self):
        return "".join(
            [
                f"graph: {print_graph(self.graph)}\n",
                f"dataframe:\n{self.dataframe}\n",
                f"exc_metrics: {self.exc_metrics}\n",
                f"inc_metrics: {self.inc_metrics}\n",
                f"default_metric: {self.default_metric}\n",
                f"metadata:\n{self.metadata}\n",
                f"profile: {self.profile}\n",
                f"profile_mapping: {self.profile_mapping}\n",
                f"statsframe:\n{print_graph(self.statsframe.graph)}\n{self.statsframe.dataframe}\n",
            ]
        )

    @staticmethod
    def thicketize_graphframe(gf, prf):
        """Necessary function to handle output from using GraphFrame readers.

        Arguments:
            gf (GraphFrame): hatchet GraphFrame object
            prf (str): Profile source of the GraphFrame

        Returns:
            (thicket): thicket object
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
            th.profile_mapping = {hash_arg: prf}
            # format metadata as a dict of dicts
            temp_meta = {}
            temp_meta[hash_arg] = th.metadata
            th.metadata = pd.DataFrame.from_dict(temp_meta, orient="index")
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
        Args:
            filename_or_stream (str or file-like): name of a Caliper output
                file in `.cali` or JSON-split format, or an open file object
                to read one
            intersection (bool): Whether to perform intersection or union (default).
            query (str): cali-query in CalQL format
        """
        return Thicket.reader_dispatch(
            GraphFrame.from_caliper, intersection, filename_or_stream, query
        )

    @staticmethod
    def from_hpctoolkit(dirname, intersection=False):
        """Create a GraphFrame using hatchet's HPCToolkit reader and use its attributes to make a new thicket.

        Arguments:
            dirname (str): parent directory of an HPCToolkit experiment.xml file
            intersection (bool): Whether to perform intersection or union (default).

        Returns:
            (thicket): new thicket containing HPCToolkit profile data
        """
        return Thicket.reader_dispatch(
            GraphFrame.from_hpctoolkit, intersection, dirname
        )

    @staticmethod
    def from_caliperreader(filename_or_caliperreader, intersection=False):
        """Helper function to read one caliper file.

        Args:
            filename_or_caliperreader (str or CaliperReader): name of a Caliper
                output file in `.cali` format, or a CaliperReader object
            intersection (bool): Whether to perform intersection or union (default).
        """
        return Thicket.reader_dispatch(
            GraphFrame.from_caliperreader, intersection, filename_or_caliperreader
        )

    @staticmethod
    def reader_dispatch(func, intersection=False, *args, **kwargs):
        """Create a thicket from a list, directory of files, or a single file.

        Args:
            func (function): reader function to be used.
            intersection (bool): Whether to perform intersection or union (default).
            *args (list): list of args. args[0] should be an object that can be read from.
        """
        ens_list = []
        obj = args[0]  # First arg should be readable object

        # Parse the input object
        if type(obj) == list:  # if a list of files
            for file in obj:
                ens_list.append(Thicket.thicketize_graphframe(func(file), file))
        elif os.path.isdir(obj):  # if directory of files
            for file in os.listdir(obj):
                f = os.path.join(obj, file)
                ens_list.append(Thicket.thicketize_graphframe(func(f), f))
        elif os.path.isfile(obj):  # if single file
            return Thicket.thicketize_graphframe(func(*args, **kwargs), args[0])
        else:
            raise TypeError(f"{type(obj)} is not a valid type to be read from.")

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

    def add_column_from_metadata_to_ensemble(self, column_name, overwrite=False):
        """Add a column from the MetadataFrame to the EnsembleFrame.

        Arguments:
            column_name (str): Name of the column from the metadataframe
            overwrite (bool): Determines overriding behavior in ensembleframe
        """
        # Add warning if column already exists in EnsembleFrame
        if column_name in self.dataframe.columns:
            if overwrite:  # Drop column to overwrite
                self.dataframe.drop(column_name, axis=1, inplace=True)
            else:  # Warn and return
                warnings.warn(
                    f'Column "{column_name}" already exists. Set "overwrite=True" to force update the column.'
                )
                return

        # Add the column to the EnsembleFrame
        self.dataframe = self.dataframe.join(
            self.metadata[column_name], on=self.dataframe.index.names[1]
        )

    def copy(self):
        """Return a partially shallow copy of the Thicket.

        See GraphFrame.copy() for more details

        Arguments:
            self (Thicket): Object to make a copy of.

        Returns:
            other (Thicket): Copy of self
                (graph ... default_metric): Same behavior as GraphFrame
                metadata (DataFrame): Pandas "non-deep" copy of dataframe
                profile (list): Copy of self's profile
                profile_mapping (dic): Copy of self's profile_mapping
                statsframe (GraphFrame): Calls GraphFrame.copy()
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
            self (Thicket): Object to make a copy of.

        Returns:
            other (Thicket): Copy of self
                (graph ... default_metric): Same behavior as GraphFrame
                metadata (DataFrame): Pandas "deep" copy of dataframe
                profile (list): Copy of self's profile
                profile_mapping (dic): Copy of self's profile_mapping
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

    def tree(self_):
        """hatchet tree() function for a thicket"""
        try:
            temp_df = self_.dataframe.drop_duplicates(subset="name").reset_index(
                level="profile"
            )
            temp_df["thicket_tree"] = -1
            return GraphFrame.tree(
                self=Thicket(graph=self_.graph, dataframe=temp_df),
                metric_column="thicket_tree",
            )
        except KeyError:
            raise NotImplementedError(
                "Printing this collection of profiles is not supported."
            )

    def unify_pair(self, other):
        """Unify two Thicket's graphs and DataFrames"""
        # Check for the same object. cheap operation since no graph walkthrough.
        if self.graph is other.graph:
            print("same graph (object)")
            return self.graph

        if (
            self.graph == other.graph
        ):  # Check for the same graph structure. Need to walk through graphs *but should still be less expensive then performing the rest of this function.*
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

    def unify_pairwise(th_list):
        """Unifies two thickets graphs and dataframes
        Ensure self and other have the same graph and same node IDs. This may
        change the node IDs in the dataframe.
        Update the graphs in the graphframe if they differ.
        """
        union_graph = th_list[0].graph
        for i in range(len(th_list)):
            for j in range(i + 1, len(th_list)):
                print(f"Unifying ({i}, {j})...")
                union_graph = th_list[i].unify_pair(th_list[j])
        return union_graph

    @staticmethod
    def unify_listwise(th_list):
        """Unify a list of Thicket's graphs and DataFrames"""
        same_graphs = (
            True  # variable to keep track of case where all graphs are the same
        )

        # GRAPH UNIFICATION
        union_graph = th_list[0].graph
        for i in range(1, len(th_list)):  # n-1 unions
            # Check to skip unecessary computation. apply short circuiting with 'or'.
            if union_graph is th_list[i].graph or union_graph == th_list[i].graph:
                print(f"Union Graph == thicket[{i}].graph")
            else:
                print(f"Unifying (Union Graph, {i})")
                same_graphs = False
                # Unify graph with current thickets graph
                union_graph = union_graph.union(th_list[i].graph)

        if (
            same_graphs
        ):  # If the graphs were all the same in the first place then there is no need to apply any node mappings.
            return union_graph

        # DATAFRAME MAPPING UPDATE
        for i in range(len(th_list)):  # n ops
            node_map = {}
            # Create a node map from current thickets graph to the union graph. This is only valid once the union graph is complete.
            union_graph.union(th_list[i].graph, node_map)
            names = th_list[i].dataframe.index.names
            th_list[i].dataframe.reset_index(inplace=True)
            th_list[i].dataframe["node"] = (
                th_list[i].dataframe["node"].apply(lambda node: node_map[id(node)])
            )  # Apply node_map mapping
            th_list[i].dataframe.set_index(names, inplace=True, drop=True)

        # After this point the graph and dataframe in each thicket is out of sync.
        # We could update the graph element in thicket to be the union graph but if the user prints out the graph how do we annotate nodes only contained in one thicket.
        return union_graph

    @staticmethod
    def _resolve_missing_indicies(th_list):
        """If at least one profile has an index that another doesn't, then issues will arise when unifying. Need to add this index to other thickets.

        Note that the value to use for the new index is set to '0' for ease-of-use, but something like 'NaN' may arguably provide more clarity.
        """
        # Create a set of all index possibilities
        idx_set = set({})
        for th in th_list:
            idx_set.update(th.dataframe.index.names)

        # Apply missing indicies to thickets
        for th in th_list:
            for idx in idx_set:
                if idx not in th.dataframe.index.names:
                    print(f"Resolving '{idx}' in thicket: ({id(th)})")
                    th.dataframe[idx] = 0
                    th.dataframe.set_index(idx, append=True, inplace=True)

    @staticmethod
    def _sync_nodes(gh, df):
        """Set the node objects to be equal in both the graph and the dataframe.
        id(graph_node) == id(df_node) after this function for nodes with equivalent hatchet nid's.
        """
        index_names = df.index.names
        df.reset_index(inplace=True)
        for graph_node in gh.traverse():
            df["node"] = df["node"].apply(
                lambda df_node: graph_node
                if (hash(graph_node) == hash(df_node))
                else df_node
            )
        df.set_index(index_names, inplace=True)

    @staticmethod
    def unify_ensemble(th_list, pairwise=False, superthicket=False):

        """Take a list of thickets and unify them into one thicket

        Arguments:
            th_list (list): list of thickets
            pairwise (bool): use the pairwise implementation of unify (use if having issues)
            superthicket (bool): whether the result is a superthicket

        Returns:
            (thicket): unified thicket
        """

        unify_graph = None
        if pairwise:
            unify_graph = Thicket.unify_pairwise(th_list)
        else:
            unify_graph = Thicket.unify_listwise(th_list)

        Thicket._resolve_missing_indicies(th_list)

        # Unify dataframe
        unify_df = pd.DataFrame()
        unify_inc_metrics = []
        unify_exc_metrics = []
        unify_metadata = pd.DataFrame()
        unify_profile = []
        unify_profile_mapping = {}

        for i, th in enumerate(th_list):  # Unification loop
            unify_inc_metrics.extend(th.inc_metrics)
            unify_exc_metrics.extend(th.exc_metrics)
            if len(th.metadata) > 0:
                curr_meta = th.metadata.copy()
                unify_metadata = pd.concat([curr_meta, unify_metadata])
                unify_metadata.index.set_names("profile", inplace=True)
            if th.profile is not None:
                unify_profile.extend(th.profile)
            if th.profile_mapping is not None:
                unify_profile_mapping.update(th.profile_mapping)
            unify_df = pd.concat([th.dataframe, unify_df])

        if superthicket:  # Operations specific to a superthicket
            unify_metadata.index.rename("thicket", inplace=True)
            unify_metadata = unify_metadata.groupby("thicket").agg(set)

        unify_metadata.sort_index(
            inplace=True
        )  # Have metadata index match ensembleframe index

        unify_df.sort_index(inplace=True)  # Sort by hatchet node id
        unify_inc_metrics = list(set(unify_inc_metrics))
        unify_exc_metrics = list(set(unify_exc_metrics))

        # Workaround for graph/df node id mismatch. (n tree nodes)x(m df nodes)x(m)
        Thicket._sync_nodes(unify_graph, unify_df)

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
    def make_superthicket(th_list, th_names=None):
        """Convert a list of thickets into a 'superthicket'. Their individual statsframes are ensembled and become the superthicket's ensembleframe.

        Arguments:
            th_list (list): list of thickets
            th_names (list): list of thicket names corresponding to the thicket list

        Returns:
            (thicket): superthicket
        """
        if th_names is None:  # Setup names list
            th_names = []
            for th in range(len(th_list)):
                th_names.append(th)
        elif len(th_names) != len(th_list):
            raise ValueError("length of names list must match length of thicket list.")
        for name in th_names:  # Check names list is valid
            if type(name) is not str and type(name) is not int:
                print(type(name))
                raise TypeError("name list must only contain integers or strings")

        for i in range(len(th_list)):
            th_list[i] = th_list[i].deepcopy()

            th_id = th_names[i]

            # Modify graph
            # Necessary so node ids match up
            th_list[i].graph = th_list[i].statsframe.graph

            # Modify the ensembleframe
            df = th_list[i].statsframe.dataframe
            df["thicket"] = th_id
            df.set_index("thicket", inplace=True, append=True)
            th_list[i].dataframe = df

            # Existing profile data no longer relevant
            th_list[i].profile = None
            th_list[i].profile_mapping = None

            # Modify metadata dataframe
            idx_name = "new_idx"
            th_list[i].metadata[idx_name] = th_id
            th_list[i].metadata.set_index(idx_name, inplace=True)

        return Thicket.unify_ensemble(th_list, superthicket=True)

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
        """Perform an intersection operation on a thicket, removing nodes that are not contained in all profiles.

        Returns:
            remaining_node_list (list): List of nodes that were not removed.
            removed_node_list (list): List of removed nodes.
        """
        # Filter the ensembleframe
        total_profiles = len(self.profile)
        remaining_node_list = []  # Needed for graph
        removed_node_list = []
        for node, new_df in self.dataframe.groupby(level=0):  # For each node
            if len(new_df) < total_profiles:  # Use profile count to make decision
                removed_node_list.append(node)
            else:
                remaining_node_list.append(node)
        self.dataframe.drop(removed_node_list, inplace=True)
        self.statsframe.dataframe.drop(
            removed_node_list, inplace=True
        )  # Propagate change to statsframe

        # Filter the graph
        self.graph.roots = list(
            set(self.graph.roots).intersection(remaining_node_list)
        )  # Remove roots
        for node in self.graph.traverse():
            # Remove children & parents that DNE in the intersection
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
        self.graph.enumerate_traverse()  # Update hatchet nid's

        return remaining_node_list, removed_node_list

    def filter_metadata(self, select_function):
        """filter thicket object based on a metadata key and propagate
        changes to the entire thicket object
        :param select_function: the filter to apply to the MetadataFrame
        :type select_function: lambda function

        :return: new thicket object with selected value
        :rtype: thicket object
        """
        if callable(select_function):
            if not self.metadata.empty:
                # create a copy of the thicket object
                new_thicket = self.copy()

                # filter MetadataFrame
                filtered_rows = new_thicket.metadata.apply(select_function, axis=1)
                new_thicket.metadata = new_thicket.metadata[filtered_rows]

                # note profile keys to filter EnsembleFrame
                profile_id = new_thicket.metadata.index.values.tolist()
                # filter EnsembleFrame based on the MetadataFrame
                new_thicket.dataframe = new_thicket.dataframe[
                    new_thicket.dataframe.index.get_level_values("profile").isin(
                        profile_id
                    )
                ]

                # create an empty StatsFrame
                new_thicket.statsframe.dataframe = pd.DataFrame(
                    data=None,
                    index=new_thicket.dataframe.index.get_level_values(
                        "node"
                    ).drop_duplicates(),
                )
            else:
                raise EmptyMetadataFrame(
                    "The provided Thicket object has an empty MetadataFrame."
                )

        else:
            raise InvalidFilter("The argument passed to filter must be a callable.")

        return new_thicket

    def filter(self, filter_func):
        raise RuntimeError(
            "Invalid function: thicket.filter(), please use thicket.filter_metadata() or thicket.filter_stats()"
        )

    def groupby(self, groupby_function):
        """create sub-thickets based on unique values in metadata column(s)

        :param groupby_function: groupby function on dataframe
        :type groupby_function: mapping, function, label, or list of labels

        :return: list containing sub-thickets
        :rtype: list[thicket object]
        """
        if not self.metadata.empty:
            # group MetadataFrame by unique values in a column
            sub_metadataframes = self.metadata.groupby(groupby_function, dropna=False)

            list_sub_thickets = []
            # for all unique groups of MetadataFrame
            for key, df in sub_metadataframes:

                # create a thicket copy
                sub_thicket = self.copy()

                # return unique group as the MetadataFrame
                sub_thicket.metadata = df

                # find profiles in current unique group & filter EnsembleFrame
                profile_id = df.index.values.tolist()
                sub_thicket.dataframe = sub_thicket.dataframe[
                    sub_thicket.dataframe.index.get_level_values("profile").isin(
                        profile_id
                    )
                ]

                # clear the StatsFrame for current unique group
                sub_thicket.statsframe.dataframe = pd.DataFrame(
                    data=None,
                    index=sub_thicket.dataframe.index.get_level_values(
                        "node"
                    ).drop_duplicates(),
                )
                list_sub_thickets.append(sub_thicket)
        else:
            raise EmptyMetadataFrame(
                "The provided Thicket object has an empty MetadataFrame."
            )

        print(len(list_sub_thickets), " Sub-Thickets created...")

        return list_sub_thickets

    def filter_stats(self, filter_function):
        """filter thicket object based on a stats column and propagate
        changes to the entire thicket object
        :param select_function: the filter to apply to the StatsFrame
        :type select_function: lambda function

        :return: new thicket object with applied filter function
        :rtype: thicket object
        """
        # copy thicket
        new_thicket = self.copy()

        # filter stats rows based on greater than restriction
        filtered_rows = new_thicket.statsframe.dataframe.apply(filter_function, axis=1)
        new_thicket.statsframe.dataframe = new_thicket.statsframe.dataframe[
            filtered_rows
        ]

        # filter ensembleframe based on filtered nodes
        filtered_nodes = new_thicket.statsframe.dataframe.index.values.tolist()
        new_thicket.dataframe = new_thicket.dataframe[
            new_thicket.dataframe.index.get_level_values("node").isin(filtered_nodes)
        ]

        # filter nodes in the graph frame based on the DataFrame nodes
        filtered_graphframe = new_thicket.squash()
        new_thicket.graph = filtered_graphframe.graph
        new_thicket.statsframe.graph = filtered_graphframe.graph

        return new_thicket


class InvalidFilter(Exception):
    """Raised when an invalid argument is passed to the filter function."""


class EmptyMetadataFrame(Exception):
    """Raised when a Thicket object argument is passed with an empty MetadataFrame to the filter function."""
