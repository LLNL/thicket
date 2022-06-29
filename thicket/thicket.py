import os

import pandas as pd

from hatchet import GraphFrame
from thicket.helpers import print_graph, all_equal


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
        th = thicket(
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


class thicket(GraphFrame):
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
        self.statsframe = pd.DataFrame(
            data=None, index=dataframe.index.get_level_values('node').drop_duplicates())

    def __str__(self):
        return ''.join([f"graph: {print_graph(self.graph)}\n",
                        f"dataframe:\n{self.dataframe}\n",
                        f"exc_metrics: {self.exc_metrics}\n",
                        f"inc_metrics: {self.inc_metrics}\n",
                        f"default_metric: {self.default_metric}\n",
                        f"metadata:\n{self.metadata}\n",
                        f"profile: {self.profile}\n",
                        f"profile_mapping: {self.profile_mapping}\n",
                        f"statsframe:\n{self.statsframe}\n",
                        ])

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
        if (type(obj) == list):  # if a list of files
            ens_list = []
            for file in obj:
                ens_list.append(thicket._from_caliperreader(file))
            return thicket.unify_ensemble(ens_list)
        elif (os.path.isdir(obj)):  # if directory of files
            ens_list = []
            for file in os.listdir(obj):
                f = os.path.join(obj, file)
                ens_list.append(thicket._from_caliperreader(f))
            return thicket.unify_ensemble(ens_list)
        elif (os.path.isfile(obj)):  # if file
            return thicket._from_caliperreader(obj)
        else:
            raise TypeError(f"{type(obj)} is not a valid type.")

    def unify(self, other):
        """Unifies two thickets graphs and dataframes
        Ensure self and other have the same graph and same node IDs. This may
        change the node IDs in the dataframe.
        Update the graphs in the graphframe if they differ.
        """
        if self.graph is other.graph:
            return

        node_map = {}
        union_graph = self.graph.union(other.graph, node_map)

        self_index_names = self.dataframe.index.names
        other_index_names = other.dataframe.index.names

        self.dataframe.reset_index(inplace=True)
        other.dataframe.reset_index(inplace=True)

        self.dataframe["node"] = self.dataframe["node"].apply(
            lambda x: node_map[id(x)])
        other.dataframe["node"] = other.dataframe["node"].apply(
            lambda x: node_map[id(x)]
        )

        # add missing rows to copy of self's dataframe in preparation for operation
        self._insert_missing_rows(other)

        self.dataframe.set_index(self_index_names, inplace=True, drop=True)
        other.dataframe.set_index(other_index_names, inplace=True, drop=True)

        self.graph = union_graph
        other.graph = union_graph

    @staticmethod
    def unify_ensemble(th_list):
        """Take a list of thickets and unify them into one thicket

        Arguments:
            th_list (list): list of thickets

        Returns:
            (thicket): unified thicket
        """
        if not all_equal([th.graph for th in th_list]):
            for i in range(len(th_list)):
                for j in range(i+1, len(th_list)):
                    th_list[i].unify(th_list[j])
                    print(f"Unifying ({i}, {j})...")

        unify_df = pd.DataFrame()
        unify_inc_metrics = []
        unify_exc_metrics = []
        unify_metadata = pd.DataFrame()
        unify_profile = []
        unify_profile_mapping = {}
        for i, th in enumerate(th_list):
            unify_inc_metrics.extend(th.inc_metrics)
            unify_exc_metrics.extend(th.exc_metrics)

            curr_meta = th.metadata.copy()
            unify_metadata = pd.concat([curr_meta, unify_metadata], sort=True)
            unify_metadata.index.set_names("profile", inplace=True)

            unify_profile.extend(th.profile)

            curr_df = th.dataframe.copy()
            unify_df = pd.concat([curr_df, unify_df], sort=True)

            unify_profile_mapping.update(th.profile_mapping)
        unify_df.sort_index(inplace=True)  # Sort by hatchet node id
        unify_inc_metrics = list(set(unify_inc_metrics))
        unify_exc_metrics = list(set(unify_exc_metrics))
        unify_th = thicket(
            graph=th_list[0].graph,
            dataframe=unify_df,
            inc_metrics=unify_inc_metrics,
            exc_metrics=unify_exc_metrics,
            metadata=unify_metadata,
            profile=unify_profile,
            profile_mapping=unify_profile_mapping,
        )
        return unify_th

    @staticmethod
    def opt_graph_unify(th_list):
        """Unify many graphs"""
        union_graph = th_list[0].graph
        for i in range(1, len(th_list)):  # n-1 unifications
            # Check to skip unecessary computation
            if union_graph is th_list[i].graph:
                continue
            union_graph = union_graph.union(th_list[i].graph)
        for th in th_list:
            th.graph = union_graph
        return union_graph

    def opt_df_uni(th_list):
        pass

    def opt_ue(th_list):
        pass
