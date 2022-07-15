# Copyright 2022 Lawrence Livermore National Security, LLC and other
# Thicket Project Developers. See the top-level LICENSE file for details.
#
# SPDX-License-Identifier: MIT

import os

import pandas as pd

from hatchet import GraphFrame
from .helpers import print_graph


def store_thicket_input_profile(func):
    """Decorator to modify a hatchet dataframe to add a profile column to track which instance the row came from.

    Arguments:
        func (Function): Function to decorate
    """

    def profile_assign(first_arg, *args, **kwargs):
        """Decorator workhorse.

        Arguments:
            first_arg (): path to profile file.
        """
        gf = func(first_arg, *args, **kwargs)
        th = Thicket(
            graph=gf.graph,
            dataframe=gf.dataframe,
            exc_metrics=gf.exc_metrics,
            inc_metrics=gf.inc_metrics,
            metadata=gf.metadata,
        )
        if th.profile is None and isinstance(first_arg, str):
            # Store used profiles and profile mappings using a hash of their string
            hash_arg = hash(first_arg)
            th.profile = [hash_arg]
            th.profile_mapping = {hash_arg: first_arg}
            # format metadata as a dict of dicts
            temp_meta = {}
            temp_meta[hash_arg] = th.metadata
            th.metadata = pd.DataFrame.from_dict(temp_meta, orient="index")
            # Add profile to dataframe index
            th.dataframe["profile"] = hash_arg
            index_names = list(th.dataframe.index.names)
            index_names.append("profile")
            th.dataframe.reset_index(inplace=True)
            th.dataframe.set_index(index_names, inplace=True)
        return th

    return profile_assign


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
            self.statsframe = GraphFrame(
                graph=self.graph,
                dataframe=pd.DataFrame(
                    data=None,
                    index=dataframe.index.get_level_values("node").drop_duplicates(),
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
    @store_thicket_input_profile
    def from_hpctoolkit(dirname):
        """Create a GraphFrame using hatchet's HPCToolkit reader and use its attributes to make a new thicket.

        Arguments:
            dirname (str): parent directory of an HPCToolkit experiment.xml file

        Returns:
            (thicket): new thicket containing HPCToolkit profile data
        """
        return GraphFrame.from_hpctoolkit(dirname)

    @staticmethod
    @store_thicket_input_profile
    def _from_caliperreader(filename_or_caliperreader):
        """Helper function to read one caliper file.

        Args:
            filename_or_caliperreader (str or CaliperReader): name of a Caliper
                output file in `.cali` format, or a CaliperReader object
        """
        return GraphFrame.from_caliperreader(filename_or_caliperreader)

    @staticmethod
    def from_caliperreader(obj):
        """Create a thicket from a list, directory of caliper files or a single file.

        Args:
            obj (list or str): obj to read from.
        """
        if type(obj) == list:  # if a list of files
            ens_list = []
            for file in obj:
                ens_list.append(Thicket._from_caliperreader(file))
            return Thicket.unify_ensemble(ens_list)
        elif os.path.isdir(obj):  # if directory of files
            ens_list = []
            for file in os.listdir(obj):
                f = os.path.join(obj, file)
                ens_list.append(Thicket._from_caliperreader(f))
            return Thicket.unify_ensemble(ens_list)
        elif os.path.isfile(obj):  # if file
            return Thicket._from_caliperreader(obj)
        else:
            raise TypeError(f"{type(obj)} is not a valid type.")

    def deepcopy(self):
        """Creates a deep copy of a Thicket and its attributes"""
        gf = GraphFrame.deepcopy(self)  # Use hatchet function

        return Thicket(
            graph=gf.graph,
            dataframe=gf.dataframe,
            exc_metrics=gf.exc_metrics,
            inc_metrics=gf.inc_metrics,
            default_metric=gf.default_metric,
            metadata=self.metadata.copy(),
            profile=self.profile,
            profile_mapping=self.profile_mapping,
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

    def unify_old(self, other):
        """Unifies two thickets graphs and dataframes
        Ensure self and other have the same graph and same node IDs. This may
        change the node IDs in the dataframe.
        Update the graphs in the graphframe if they differ.
        """

        # Check for the same object. cheap operation since no graph walkthrough.
        if self.graph is other.graph:
            print("same graph (object)")
            return

        if (
            self.graph == other.graph
        ):  # Check for the same graph structure. Need to walk through graphs *but should still be less expensive then performing the rest of this function.*
            print("same graph (structure)")
            return

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

        return self.graph

    @staticmethod
    def unify_new(th_list):
        same_graphs = (
            True  # variable to keep track of case where all graphs are the same
        )
        node_map = {}
        index_name_list = []

        # GRAPH UNIFICATION
        union_graph = th_list[0].graph
        for i in range(1, len(th_list)):  # n-1 unions
            print(f"Unifying (Union Graph, {i})")
            # Check to skip unecessary computation. apply short circuiting with 'or'.
            if union_graph is th_list[i].graph or union_graph == th_list[i].graph:
                continue
            else:
                same_graphs = False
            union_graph = union_graph.union(th_list[i].graph, node_map)

        if same_graphs:  # indicates rest of function unecessary
            return

        # DATAFRAME MAPPING UPDATE
        for i in range(len(th_list)):  # n ops
            index_name_list.append(th_list[i].dataframe.index.names)
            th_list[i].dataframe.reset_index(inplace=True)
            th_list[i].dataframe["node"] = (
                th_list[i].dataframe["node"].apply(lambda x: node_map[id(x)])
            )
            th_list[i].dataframe.set_index(index_name_list[i], inplace=True)

        return union_graph

    @staticmethod
    def unify_ensemble(th_list, old=False, superthicket=False):
        """Take a list of thickets and unify them into one thicket

        Arguments:
            th_list (list): list of thickets
            old (bool): use the old implementation of unify (use if having issues)
            superthicket (bool): whether the result is a superthicket

        Returns:
            (thicket): unified thicket
        """
        unify_graph = None
        if old:
            for i in range(len(th_list)):
                for j in range(i + 1, len(th_list)):
                    print(f"Unifying ({i}, {j})...")
                    unify_graph = th_list[i].unify_old(th_list[j])
        else:
            unify_graph = Thicket.unify_new(th_list)

        if unify_graph is None:  # Case where all graphs are the same
            unify_graph = th_list[0].graph

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
                unify_metadata = pd.concat([curr_meta, unify_metadata], sort=True)
                unify_metadata.index.set_names("profile", inplace=True)
            if th.profile is not None:
                unify_profile.extend(th.profile)
            if th.profile_mapping is not None:
                unify_profile_mapping.update(th.profile_mapping)
            curr_df = th.dataframe.copy()
            unify_df = pd.concat([curr_df, unify_df])

        if superthicket:  # Operations specific to a superthicket
            unify_metadata.index.rename("thicket", inplace=True)
            unify_metadata = unify_metadata.groupby("thicket").agg(set)

        unify_df.sort_index(inplace=True)  # Sort by hatchet node id
        unify_inc_metrics = list(set(unify_inc_metrics))
        unify_exc_metrics = list(set(unify_exc_metrics))

        unify_th = Thicket(
            graph=unify_graph,
            dataframe=unify_df,
            inc_metrics=unify_inc_metrics,
            exc_metrics=unify_exc_metrics,
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
