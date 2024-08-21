# Copyright 2022 Lawrence Livermore National Security, LLC and other
# Thicket Project Developers. See the top-level LICENSE file for details.
#
# SPDX-License-Identifier: MIT

import copy
import os
import pickle
import sys
import json
import warnings
import itertools
from collections import defaultdict, OrderedDict
from hashlib import md5

import pandas as pd
import numpy as np
from hatchet import GraphFrame
from hatchet.frame import Frame
from hatchet.graph import Graph
from hatchet.node import Node
from hatchet.query import QueryEngine
from thicket.query import (
    Query,
    ObjectQuery,
    parse_string_dialect,
    is_hatchet_query,
    is_old_style_query,
)
import tqdm

from thicket.ensemble import Ensemble
from thicket.utils import _fill_perfdata

try:
    from .ncu import NCUReader
except ModuleNotFoundError:
    pass
import thicket.helpers as helpers
from .groupby import GroupBy
from .utils import (
    verify_thicket_structures,
    check_duplicate_metadata_key,
    validate_profile,
    validate_nodes,
)
from .external.console import ThicketRenderer


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
        statsframe_ops_cache=None,
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
        self.query_engine = QueryEngine()
        self.performance_cols = helpers._get_perf_columns(self.dataframe)

        if statsframe_ops_cache is None:
            self.statsframe_ops_cache = {}
        else:
            self.statsframe_ops_cache = statsframe_ops_cache

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
    def profile_hasher(obj, hex_len=8):
        """Convert an object to a profile hash for Thicket.

        Arguments:
            obj (object): hashable object
            hex_len (int): length of the hex string before being converted to an integer.

        Returns:
            (int): hash of the object
        """

        return int(md5(obj.encode("utf-8")).hexdigest()[:hex_len], 16)

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
            # Store used profiles and profile mappings using a truncated md5 hash of their string.
            # Resulting int hash will be at least hex_length digits and theoretically up to
            # ceil(log_10(16^n - 1)) digits after conversion.

            hash_arg = Thicket.profile_hasher(prf)
            th.profile = [hash_arg]
            th.profile_mapping = OrderedDict({hash_arg: prf})

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
    def thicketize_timeseries_graphframe(gf, iter_column="loop.start_iteration"):
        """Necessary function to handle output from using GraphFrame readers.

        Arguments:
            gf (GraphFrame): hatchet GraphFrame object
            iter_column (str): column name on which we split timeseries file
                will be renamed to "iteration" and deleted, default is "loop.start_iteration"

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

        # format metadata as a dict of dicts
        temp_meta = {}
        temp_meta["iteration"] = th.metadata
        th.metadata = pd.DataFrame.from_dict(temp_meta, orient="index")
        th.metadata.index.set_names("iteration", inplace=True)

        # Use iter_column to set iteration dataframe index then delete it
        iteration = int(th.dataframe[iter_column].dropna().max())
        th.dataframe.drop(columns=[iter_column], inplace=True)
        th.dataframe["iteration"] = iteration
        index_names = list(th.dataframe.index.names)
        index_names.insert(1, "iteration")
        th.dataframe.reset_index(inplace=True)
        th.dataframe.set_index(index_names, inplace=True)

        return th

    @staticmethod
    def from_pickle(filename, **kwargs):
        """Read in a Thicket from a pickle file."""
        return pickle.load(open(filename, "rb"), **kwargs)

    def to_pickle(self, filename, **kwargs):
        """Write a Thicket to a pickle file."""
        pickle.dump(self, open(filename, "wb"), **kwargs)

    @staticmethod
    def from_caliper(
        filename_or_stream,
        query=None,
        intersection=False,
        fill_perfdata=True,
        disable_tqdm=False,
    ):
        """Read in a Caliper .cali or .json file.

        Arguments:
            filename_or_stream (str or file-like): name of a Caliper output file in
                `.cali` or JSON-split format, or an open file object to read one
            query (str): cali-query in CalQL format
            intersection (bool): whether to perform intersection or union (default)
            fill_perfdata (bool): whether to fill missing performance data with NaNs
            disable_tqdm (bool): whether to display tqdm progress bar
        """
        return Thicket.reader_dispatch(
            GraphFrame.from_caliper,
            intersection,
            fill_perfdata,
            disable_tqdm,
            filename_or_stream,
            query,
        )

    @staticmethod
    def from_hpctoolkit(
        dirname, intersection=False, fill_perfdata=True, disable_tqdm=False
    ):
        """Create a GraphFrame using hatchet's HPCToolkit reader and use its attributes
        to make a new thicket.

        Arguments:
            dirname (str): parent directory of an HPCToolkit experiment.xml file
            intersection (bool): whether to perform intersection or union (default)
            fill_perfdata (bool): whether to fill missing performance data with NaNs
            disable_tqdm (bool): whether to display tqdm progress bar

        Returns:
            (thicket): new thicket containing HPCToolkit profile data
        """
        return Thicket.reader_dispatch(
            GraphFrame.from_hpctoolkit,
            intersection,
            fill_perfdata,
            disable_tqdm,
            dirname,
        )

    @staticmethod
    def from_caliperreader(
        filename_or_caliperreader,
        intersection=False,
        fill_perfdata=True,
        disable_tqdm=False,
    ):
        """Helper function to read one caliper file.

        Arguments:
            filename_or_caliperreader (str or CaliperReader): name of a Caliper output
                file in `.cali` format, or a CaliperReader object
            intersection (bool): whether to perform intersection or union (default)
            fill_perfdata (bool): whether to fill missing performance data with NaNs
            disable_tqdm (bool): whether to display tqdm progress bar
        """
        return Thicket.reader_dispatch(
            GraphFrame.from_caliperreader,
            intersection,
            fill_perfdata,
            disable_tqdm,
            filename_or_caliperreader,
        )

    @staticmethod
    def from_literal(graph_dict):
        """Create a Thicket from a list of dictionarires.

        Arguments:
            graph_dict (list): list of dictionaries representing a graph

        Returns:
            (Thicket): new Thicket

        Example:
            graph_dict = [
                {
                    "frame": {"name": "Foo", "type": "function"},
                    "metrics": {"memory": 30.0, "time": 0.1},
                    "children": [
                        {
                            "frame": {"name": "Bar", "type": "function"},
                            "metrics": {"memory": 11.0, "time": 5.0},
                            "children": [],
                        },
                    ],
                },
                {
                    "frame": {"name": "Baz", "type": "function"},
                    "metrics": {"memory": 6.0, "time": 5.0},
                    "children": [],
                },
            ]
        """
        profile = str(graph_dict)

        tk = Thicket.thicketize_graphframe(GraphFrame.from_literal(graph_dict), profile)

        return tk

    def from_timeseries(
        filename_or_caliperreader, level="loop.start_iteration", intersection=False
    ):
        """Read in a single Caliper .cali file that has timeseries records
        gets split into a list of graphframes by hatchet.

        Arguments:
            filename_or_stream (str or file-like): name of a Caliper timeseries output file in
                `.cali` format, or an open file object to read one
            level (str): level to split the timeseries, default "loop.start_iteration"
            intersection (bool): whether to perform intersection or union (default)
        """
        # if we are reading a timeseries file we will expect a list back from the reader
        ens_list = []
        gf_list = GraphFrame.from_timeseries(filename_or_caliperreader)
        for gf in gf_list:
            ens_list.append(Thicket.thicketize_timeseries_graphframe(gf, level))

        # Perform unify ensemble
        thicket_object = Thicket.concat_thickets(ens_list)
        return thicket_object

    @staticmethod
    def reader_dispatch(
        func, intersection, fill_perfdata, disable_tqdm, *args, **kwargs
    ):

        """Create a thicket from a list, directory of files, or a single file.

        Arguments:
            func (function): reader function to be used
            intersection (bool): whether to perform intersection or union (default).
            tdmq_output (bool): whether to display tqdm progress bar
            args (list): list of args; args[0] should be an object that can be read from
        """

        ens_list = []
        obj = args[0]  # First arg should be readable object
        extra_args = args[1:]
        pbar_desc = "(1/2) Reading Files"

        # Error checking
        if isinstance(obj, (list, tuple)):
            if len(obj) == 0:
                raise ValueError("Iterable must contain at least one file")
            for file in obj:
                if not os.path.isfile(file):
                    raise FileNotFoundError("File '" + file + "' not found")
        elif isinstance(obj, str):
            if not os.path.isfile(obj):
                raise FileNotFoundError("File '" + obj + "' not found")
        else:
            raise TypeError(
                "'" + str(type(obj).__name__) + "' is not a valid type to be read from"
            )

        # Parse the input object
        # if a list of files
        if isinstance(obj, (list, tuple)):
            pbar = tqdm.tqdm(obj, disable=disable_tqdm)
            for file in pbar:
                pbar.set_description(pbar_desc)
                ens_list.append(
                    Thicket.thicketize_graphframe(
                        func(file, *extra_args, **kwargs), file
                    )
                )
        # if directory of files
        elif os.path.isdir(obj):
            pbar = tqdm.tqdm(os.listdir(obj), disable=disable_tqdm)
            for file in pbar:
                pbar.set_description(pbar_desc)
                f = os.path.join(obj, file)
                ens_list.append(
                    Thicket.thicketize_graphframe(func(f, *extra_args, **kwargs), f)
                )
        # if single file
        elif os.path.isfile(obj):
            return Thicket.thicketize_graphframe(func(*args, **kwargs), args[0])

        # Perform ensembling operation
        calltree = "union"
        if intersection:
            calltree = "intersection"
        # Consider a binary tree with t leaves (Thickets) and 2t-1 total nodes (Full tree)
        # This implies t-1 parents (Knuth 1997).
        # Each non-leaf Thicket (parent) is a result of a concat_thickets, therefore
        # we have t-1 concat_thickets operations. t == len(ens_list)
        pbar = tqdm.tqdm(total=len(ens_list) - 1, disable=disable_tqdm)
        # We start with t Thickets with 1 prof each until 1 Thicket with t profs.
        # Each iteration is concatenating 2 Thickets with n, m profs into 1 Thicket
        # with n+m profs, appending the new Thicket to the back of the ens_list.
        while len(ens_list) > 1:
            pbar.set_description("(2/2) Creating Thicket")
            new_tk = Thicket.concat_thickets(
                thickets=[ens_list.pop(0), ens_list.pop(0)],
                axis="index",
                calltree=calltree,
                fill_perfdata=fill_perfdata,
                disable_tqdm=disable_tqdm,
            )
            ens_list.append(new_tk)
            pbar.update(1)

        return ens_list.pop(0)

    @staticmethod
    def concat_thickets(
        thickets, axis="index", calltree="union", disable_tqdm=False, **kwargs
    ):
        """Concatenate thickets together on index or columns.

        The calltree can either be unioned or intersected which will affect the other structures.

        Arguments:
            thickets (list): list of thicket objects
            axis (str): axis to concatenate on -> "index" or "column"
            calltree (str): calltree to use -> "union" or "intersection"

        Keyword Arguments:
            fill_perfdata (bool): (if axis="index") Whether to fill missing performance data with NaNs
            headers (list): (if axis="columns") List of headers to use for the new columnar multi-index
            metadata_key (str): (if axis="columns") Name of the column from the metadata tables to replace the 'profile'
                index. If no argument is provided, it is assumed that there is no profile-wise
                relationship between the thickets.

        Returns:
            (thicket): concatenated thicket
        """

        def _index(
            thickets,
            from_statsframes=False,
            fill_perfdata=True,
            disable_tqdm=disable_tqdm,
        ):
            thicket_parts = Ensemble._index(
                thickets=thickets,
                from_statsframes=from_statsframes,
                fill_perfdata=fill_perfdata,
                disable_tqdm=disable_tqdm,
            )

            return Thicket(
                graph=thicket_parts[0],
                dataframe=thicket_parts[1],
                exc_metrics=thicket_parts[2],
                inc_metrics=thicket_parts[3],
                metadata=thicket_parts[4],
                profile=thicket_parts[5],
                profile_mapping=thicket_parts[6],
                # Cache must be cleared since two caches cannot be merged
                statsframe_ops_cache={},
            )

        def _columns(
            thickets, headers=None, metadata_key=None, disable_tqdm=disable_tqdm
        ):
            combined_thicket = Ensemble._columns(
                thickets=thickets,
                headers=headers,
                metadata_key=metadata_key,
                disable_tqdm=disable_tqdm,
            )
            # Clear cache since keys become invalid due to multiindex additions
            combined_thicket.statsframe_ops_cache = {}

            return combined_thicket

        if calltree not in ["union", "intersection"]:
            raise ValueError("calltree must be 'union' or 'intersection'")

        if axis == "index":
            ct = _index(thickets, **kwargs)
        elif axis == "columns":
            ct = _columns(thickets, **kwargs)
        else:
            raise ValueError("axis must be 'index' or 'columns'")

        if calltree == "intersection":
            ct = ct.intersection()

        return ct

    @staticmethod
    def columnar_join(thicket_list, header_list=None, metadata_key=None):
        raise ValueError(
            "columnar_join is deprecated. Use 'concat_thickets(axis='columns'...)' instead."
        )

    @staticmethod
    def unify_ensemble(th_list, from_statsframes=False):
        raise ValueError(
            "unify_ensemble is deprecated. Use 'concat_thickets(axis='index'...)' instead."
        )

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

    def add_ncu(self, ncu_report_mapping, chosen_metrics=None, overwrite=False):
        """Add NCU data into the PerformanceDataFrame

        Arguments:
            ncu_report_mapping (dict): mapping from NCU report file to profile
            chosen_metrics (list): list of metrics to sub-select from NCU report
            overwrite (bool): whether to overwrite existing columns in the Thicket.DataFrame
        """

        def _rep_agg_func(col):
            """Aggregate function for repition data.

            Arguments:
                col (pd.Series): column of data
            """
            rollup_operation = rollup_dict[col.name]
            agg_func = ncureader.rollup_operations[rollup_operation]
            if agg_func is not None and pd.api.types.is_numeric_dtype(col):
                return agg_func(col)
            else:
                return col[0]

        # Check if chosen_metrics are in the dataframe
        dupe_cols = [col for col in chosen_metrics if col in self.dataframe.columns]
        if overwrite:
            self.dataframe = self.dataframe.drop(columns=dupe_cols)
        elif not overwrite and len(dupe_cols) > 0:
            raise ValueError(
                f"Columns {dupe_cols} already exist in the performance data table. Set overwrite=True to overwrite."
            )

        # Initialize reader
        ncureader = NCUReader()

        # Dictionary of NCU data
        data_dict, rollup_dict = ncureader._read_ncu(self, ncu_report_mapping)

        # Create empty df
        ncu_df = pd.DataFrame()
        # Loop to aggregate data across reps
        for node_profile, rep_data in data_dict.items():
            # Aggregate data using _rep_agg_func
            agg_data = pd.DataFrame.from_records(rep_data).agg(_rep_agg_func)
            # Add node and profile
            agg_data["node"] = node_profile[0]
            agg_data["profile"] = node_profile[1]
            # Append to main df
            ncu_df = pd.concat([ncu_df, pd.DataFrame([agg_data])], ignore_index=True)
        ncu_df = ncu_df.set_index(["node", "profile"])

        # Apply chosen metrics
        if chosen_metrics:
            ncu_df = ncu_df[chosen_metrics]

        # Join NCU DataFrame into Thicket
        self.dataframe = self.dataframe.join(
            ncu_df,
            how="outer",
            sort=True,
            lsuffix="_left",
            rsuffix="_right",
        )

    def metadata_column_to_perfdata(self, metadata_key, overwrite=False, drop=False):
        """Add a column from the metadata table to the performance data table.

        Arguments:
            metadata_key (str): Name of the column from the metadata table
            overwrite (bool): Determines overriding behavior in performance data table
            drop (bool): Whether to drop the column from the metadata table afterwards
        """
        # Add warning if column already exists in performance data table
        if metadata_key in self.dataframe.columns:
            # Drop column to overwrite, otherwise warn and return
            if overwrite:
                self.dataframe.drop(metadata_key, axis=1, inplace=True)
            else:
                warnings.warn(
                    "Column "
                    + metadata_key
                    + " already exists. Set 'overwrite=True' to force update the column."
                )
                return

        # Add the column to the performance data table
        self.dataframe = self.dataframe.join(
            self.metadata[metadata_key], on=self.dataframe.index.names[1]
        )

        # Drop column
        if drop:
            self.metadata.drop(metadata_key, axis=1, inplace=True)

    def squash(self, update_inc_cols=True, new_statsframe=True):
        """Rewrite the Graph to include only nodes present in the performance
        data table's rows.

        This can be used to simplify the Graph, or to normalize Graph indexes between
        two Thickets.

        Arguments:
            update_inc_cols (boolean, optional): if True, update inclusive columns.
            new_statsframe (boolean, optional): if True, create a new statsframe from the new dataframe. Only set to False if the statsframe nodes equal the dataframe nodes.

        Returns:
            (thicket): a newly squashed Thicket object
        """

        #####
        # Hatchet's squash code
        #####
        index_names = self.dataframe.index.names
        self.dataframe.reset_index(inplace=True)

        # create new nodes for each unique node in the old dataframe
        old_to_new = {n: n.copy() for n in set(self.dataframe["node"])}
        for i in old_to_new:
            old_to_new[i]._hatchet_nid = i._hatchet_nid

        # Maintain sets of connections to make for each old node.
        # Start with old -> new mapping and update as we traverse subgraphs.
        connections = defaultdict(lambda: set())
        connections.update({k: {v} for k, v in old_to_new.items()})

        new_roots = []  # list of new roots

        # connect new nodes to children according to transitive
        # relationships in the old graph.
        def rewire(node, new_parent, visited):
            # make all transitive connections for the node we're visiting
            for n in connections[node]:
                if new_parent:
                    # there is a parent in the new graph; connect it
                    if n not in new_parent.children:
                        new_parent.add_child(n)
                        n.add_parent(new_parent)

                elif n not in new_roots:
                    # this is a new root
                    new_roots.append(n)

            new_node = old_to_new.get(node)
            transitive = set()
            if node not in visited:
                visited.add(node)
                for child in node.children:
                    transitive |= rewire(child, new_node or new_parent, visited)

            if new_node:
                # since new_node exists in the squashed graph, we only
                # need to connect new_node
                return {new_node}
            else:
                # connect parents to the first transitively reachable
                # new_nodes of nodes we're removing with this squash
                connections[node] |= transitive
                return connections[node]

        # run rewire for each root and make a new graph
        visited = set()
        for root in self.graph.roots:
            rewire(root, None, visited)
        graph = Graph(new_roots)
        if self.graph.node_ordering:
            graph.node_ordering = True
        graph.enumerate_traverse()

        # reindex new dataframe with new nodes
        df = self.dataframe.copy()
        df["node"] = df["node"].apply(lambda x: old_to_new[x])

        # at this point, the graph is potentially invalid, as some nodes
        # may have children with identical frames.
        merges = graph.normalize()
        df["node"] = df["node"].apply(lambda n: merges.get(n, n))

        self.dataframe.set_index(index_names, inplace=True)
        df.set_index(index_names, inplace=True)
        # create dict that stores aggregation function for each column
        agg_dict = {}
        for col in df.columns.tolist():
            if col in self.exc_metrics + self.inc_metrics:
                # use min_count=1 (default is 0) here, so sum of an all-NA
                # series is NaN, not 0
                # when min_count=1, sum([NaN, NaN)] = NaN
                # when min_count=0, sum([NaN, NaN)] = 0
                agg_dict[col] = lambda x: x.sum(min_count=1)
            else:
                agg_dict[col] = lambda x: x.iloc[0]

        # perform a groupby to merge nodes with the same callpath
        agg_df = df.groupby(index_names).agg(agg_dict)
        agg_df.sort_index(inplace=True)

        #####
        # Thicket's squash code
        #####
        multiindex = False
        if isinstance(self.statsframe.dataframe.columns, pd.MultiIndex):
            multiindex = True

        if new_statsframe:
            stats_df = helpers._new_statsframe_df(agg_df, multiindex=multiindex)
            sframe = GraphFrame(
                graph=graph,
                dataframe=stats_df,
            )
        else:
            # Update the node objects in the old statsframe.
            sframe = self.statsframe
            sframe.dataframe = sframe.dataframe.reset_index()
            replace_dict = {}
            for node in sframe.dataframe["node"]:
                if node in old_to_new:
                    replace_dict[node] = old_to_new[node]
            sframe.dataframe["node"] = sframe.dataframe["node"].replace(replace_dict)
            sframe.dataframe = sframe.dataframe.set_index("node")

        new_tk = Thicket(
            graph,
            agg_df,
            exc_metrics=self.exc_metrics,
            inc_metrics=self.inc_metrics,
            default_metric=self.default_metric,
            metadata=self.metadata,
            profile=self.profile,
            profile_mapping=self.profile_mapping,
            statsframe=sframe,
            statsframe_ops_cache=self.statsframe_ops_cache,
        )

        if update_inc_cols:
            new_tk.update_inclusive_columns()

        new_tk._sync_profile_components(new_tk.dataframe)
        validate_profile(new_tk)
        validate_nodes(new_tk)

        return new_tk

    def copy(self):
        """Return a partially shallow copy of the Thicket.

        See GraphFrame.copy() for more details

        Arguments:
            self (Thicket): object to make a copy of

        Returns:
            other (thicket): copy of self
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
            statsframe_ops_cache=self.statsframe_ops_cache.copy(),
        )

    def deepcopy(self):
        """Return a deep copy of the Thicket.

        See GraphFrame.deepcopy() for more details

        Arguments:
            self (Thicket): object to make a copy of

        Returns:
            other (thicket): copy of self
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
            statsframe=GraphFrame(
                graph=gf.graph, dataframe=self.statsframe.dataframe.copy(deep=True)
            ),
            statsframe_ops_cache=self.statsframe_ops_cache.copy(),
        )

    def tree(
        self,
        metric_column=None,
        annotation_column=None,
        precision=3,
        name_column="name",
        expand_name=False,
        context_column="file",
        rank=0,
        thread=0,
        depth=10000,
        highlight_name=False,
        colormap="RdYlGn",
        invert_colormap=False,
        colormap_annotations=None,
        render_header=True,
        min_value=None,
        max_value=None,
        indices=None,
    ):
        """Visualize the Thicket as a tree

        Arguments:
            metric_column (str, tuple, list, optional): Columns to use the metrics from. Defaults to None.
            annotation_column (str, optional): Column to use as an annotation. Defaults to None.
            precision (int, optional): Precision of shown numbers. Defaults to 3.
            name_column (str, optional): Column of the node name. Defaults to "name".
            expand_name (bool, optional): Limits the lenght of the node name. Defaults to False.
            context_column (str, optional): Shows the file this function was called in (Available with HPCToolkit). Defaults to "file".
            rank (int, optional): Specifies the rank to take the data from. Defaults to 0.
            thread (int, optional): Specifies the thread to take the data from. Defaults to 0.
            depth (int, optional): Sets the maximum depth of the tree. Defaults to 10000.
            highlight_name (bool, optional): Highlights the names of the nodes. Defaults to False.
            colormap (str, optional): Specifies a colormap to use. Defaults to "RdYlGn".
            invert_colormap (bool, optional): Reverts the chosen colormap. Defaults to False.
            colormap_annotations (str, list, dict, optional): Either provide the name of a colormap, a list of colors to use or a dictionary which maps the used annotations to a color. Defaults to None.
            render_header (bool, optional): Shows the Preamble. Defaults to True.
            min_value (int, optional): Overwrites the min value for the coloring legend. Defaults to None.
            max_value (int, optional): Overwrites the max value for the coloring legend. Defaults to None.
            indices(tuple, list, optional): Index/indices to display on the DataFrame. Defaults to None.

        Returns:
            (str): String representation of the tree, ready to print
        """
        color = sys.stdout.isatty()
        shell = None
        if metric_column is None:
            metric_column = self.default_metric

        if name_column == "name" and isinstance(self.dataframe.columns, pd.MultiIndex):
            name_column = (name_column, "")

        if color is False:
            try:
                import IPython

                shell = IPython.get_ipython().__class__.__name__
            except ImportError:
                pass
            # Test if running in a Jupyter notebook or qtconsole
            if shell == "ZMQInteractiveShell":
                color = True

        if sys.version_info.major == 2:
            unicode = False
        elif sys.version_info.major == 3:
            unicode = True

        if indices is None:
            # Create slice out of first values found starting after the first index.
            indices = self.dataframe.index[0][1:]
        elif isinstance(indices, tuple):
            pass
        elif isinstance(indices, list):  # Convert list to tuple
            indices = tuple(indices)
        else:  # Support for non-iterable types (int, str, ...)
            try:
                indices = tuple([indices])
            except TypeError:
                raise TypeError(
                    f"Value provided to 'indices' = {indices} is an unsupported type {type(indices)}"
                )
        # For tree legend
        idx_dict = {
            self.dataframe.index.names[k + 1]: indices[k] for k in range(len(indices))
        }
        # Slices the DataFrame to simulate a single-level index
        try:
            # Select only columns used by tree for efficiency in _fill_perfdata
            df_cols = [
                col
                for col in [
                    *(
                        metric_column
                        if isinstance(metric_column, list)
                        else [metric_column]
                    ),
                    annotation_column,
                    name_column,
                    context_column,
                ]
                if col in self.dataframe.columns
            ]
            slice_df = self.dataframe.loc[:, df_cols]
            # _fill_perfdata to make sure number of nodes in df == graph
            slice_df = _fill_perfdata(slice_df)
            slice_df = (
                slice_df.loc[(slice(None),) + indices, :]
                .reset_index()
                .set_index("node")
            )
        except KeyError:
            missing_indices = {
                list(idx_dict.keys())[i]: idx
                for i, idx in enumerate(indices)
                if all(idx not in df_idx[1:] for df_idx in self.dataframe.index)
            }
            raise KeyError(
                f"The indices, {missing_indices}, do not exist in the index 'self.dataframe.index'"
            )
        # Check for compatibility
        if len(slice_df) != len(self.graph):
            raise KeyError(
                f"Either dataframe cannot be represented as a single index or provided slice, '{indices}' results in a multi-index. See self.dataframe.loc[(slice(None),)+{indices},{metric_column}]"
            )

        # Prep DataFrame by filling None rows in the "name" column with the node's name.
        slice_df["name"] = [
            n.frame["name"] for n in slice_df.index.get_level_values("node")
        ]

        return ThicketRenderer(unicode=unicode, color=color).render(
            self.graph.roots,
            slice_df,
            metric_column=metric_column,
            annotation_column=annotation_column,
            precision=precision,
            name_column=name_column,
            expand_name=expand_name,
            context_column=context_column,
            rank=rank,
            thread=thread,
            depth=depth,
            highlight_name=highlight_name,
            colormap=colormap,
            invert_colormap=invert_colormap,
            colormap_annotations=colormap_annotations,
            render_header=render_header,
            min_value=min_value,
            max_value=max_value,
            indices=idx_dict,
        )

    @staticmethod
    def from_statsframes(tk_list, metadata_key=None, disable_tqdm=False):
        """Compose a list of Thickets with data in their statsframes.

        The Thicket's individual aggregated statistics tables are ensembled and become the
        new Thickets performance data table. This also results in aggregation of the metadata.

        Arguments:
            tk_list (list): list of thickets
            metadata_key (str, optional): name of the metadata column to use as
                the new second-level index. Uses the first value so this only makes
                sense if provided column is all equal values and each thicket's columns
                differ in value.

        Returns:
            (thicket): New Thicket object.
        """
        # Pre-check of data structures
        for tk in tk_list:
            verify_thicket_structures(
                tk.dataframe, index=["node", "profile"]
            )  # Required for deepcopy operation
            verify_thicket_structures(
                tk.statsframe.dataframe, index=["node"]
            )  # Required for deepcopy operation

        # Setup names list
        tk_names = []
        if metadata_key is None:
            idx_name = "profile"  # Set index name to general "profile"
            for i in range(len(tk_list)):
                tk_names.append(i)
        else:  # metadata_key was provided.
            check_duplicate_metadata_key(tk_list, metadata_key)
            idx_name = metadata_key  # Set index name to metadata_key
            for tk in tk_list:
                # Get name from metadata table
                name_list = tk.metadata[metadata_key].tolist()

                if len(set(name_list)) > 1:
                    warnings.warn(
                        f"Multiple values for name {name_list} at thicket.metadata['{metadata_key}']. Only the first value will be used for the new DataFrame index."
                    )
                tk_names.append(name_list[0])

        tk_copy_list = []
        for i in range(len(tk_list)):
            tk_copy = tk_list[i].deepcopy()

            tk_id = tk_names[i]

            # Modify graph. Necessary so node ids match up
            tk_copy.graph = tk_copy.statsframe.graph

            # Modify the performance data table
            df = tk_copy.statsframe.dataframe
            df[idx_name] = tk_id
            df.set_index(idx_name, inplace=True, append=True)
            tk_copy.dataframe = df

            # Adjust profile and profile_mapping
            tk_copy.profile = [tk_id]
            profile_paths = list(tk_copy.profile_mapping.values())
            tk_copy.profile_mapping = OrderedDict({tk_id: profile_paths})

            # Modify metadata dataframe
            tk_copy.metadata[idx_name] = tk_id
            tk_copy.metadata.set_index(idx_name, inplace=True)

            def _agg_to_set(obj):
                """Aggregate values in 'obj' into a set to remove duplicates."""
                if len(obj) <= 1:
                    return obj
                else:
                    if isinstance(obj.iloc[0], list) or isinstance(obj.iloc[0], set):
                        _set = set(tuple(i) for i in obj)
                    else:
                        _set = set(obj)
                    if len(_set) == 1:
                        return _set.pop()
                    else:
                        return _set

            # Execute aggregation
            tk_copy.metadata = tk_copy.metadata.groupby(idx_name).agg(_agg_to_set)

            # Append copy to list
            tk_copy_list.append(tk_copy)

        return Thicket.concat_thickets(
            tk_copy_list, from_statsframes=True, disable_tqdm=disable_tqdm
        )

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
            (thicket): intersected thicket
        """

        # Check for padded perfdata
        if self.dataframe["name"].isnull().any():
            # Row that didn't exist will contain "None" in the name column.
            query = Query().match(
                ".", lambda df: df["name"].apply(lambda n: n is not None).all()
            )
        else:
            # If perfdata not padded
            query = Query().match(".", lambda df: len(df) == len(self.profile))

        intersected_th = self.query(query)

        return intersected_th

    def filter_metadata(self, select_function):
        """Filter thicket object based on a metadata key.

        Changes are propagated to the entire thicket object.

        Arguments:
            select_function (lambda function): filter to apply to the metadata table

        Returns:
            (thicket): new thicket object with selected value
        """
        # Error checks
        if not callable(select_function):
            raise InvalidFilter("The argument passed to filter must be a callable.")
        if self.metadata.empty:
            raise EmptyMetadataTable(
                "The provided Thicket object has an empty MetadataTable."
            )
        # check only 1 index in metadata
        if isinstance(self.metadata.index, pd.MultiIndex):
            raise TypeError("The metadata index must be single-level.")
        # Add warning if filtering on multi-index columns
        if isinstance(self.metadata.columns, pd.MultiIndex):
            warnings.warn(
                "Filtering on MultiIndex columns will impact the entire row, not just the subsection of the provided MultiIndex."
            )

        # Get index name
        index_name = self.metadata.index.name

        # create a deepcopy of the thicket object
        new_thicket = self.deepcopy()

        # filter metadata table
        filtered_rows = new_thicket.metadata.apply(select_function, axis=1)
        if not filtered_rows.any():
            raise EmptyMetadataTable(
                "The provided filter function resulted in an empty MetadataTable."
            )
        new_thicket.metadata = new_thicket.metadata[filtered_rows]

        # note index keys to filter performance data table
        index_id = new_thicket.metadata.index.values.tolist()
        # filter performance data table based on the metadata table
        new_thicket.dataframe = new_thicket.dataframe[
            new_thicket.dataframe.index.get_level_values(index_name).isin(index_id)
        ]

        # create an empty aggregated statistics table with the name column
        new_thicket.statsframe.dataframe = helpers._new_statsframe_df(
            new_thicket.dataframe
        )

        new_thicket._sync_profile_components(new_thicket.metadata)
        validate_profile(new_thicket)

        # If fill_perfdata is False, may need to squash
        if len(new_thicket.graph) != len(
            new_thicket.dataframe.index.get_level_values("node").unique()
        ):
            new_thicket = new_thicket.squash()

        return new_thicket

    def filter_profile(self, profile_list):
        """Filter thicket object based on a list of profiles.

        Arguments:
            profile_list (list): list of profiles to filter on

        Returns:
            (thicket): new thicket object with selected profiles
        """
        new_thicket = self.deepcopy()

        new_thicket._sync_profile_components(profile_list)
        validate_profile(new_thicket)

        if len(new_thicket.graph) != len(
            new_thicket.dataframe.index.get_level_values("node").unique()
        ):
            new_thicket = new_thicket.squash()

        return new_thicket

    def filter(self, filter_func):
        """Overloaded generic filter function.

        Provides a helper message for using the thicket filter functions.
        """
        raise RuntimeError(
            "Invalid function: thicket.filter(), please use thicket.filter_metadata() or thicket.filter_stats()"
        )

    def query(
        self, query_obj, squash=True, update_inc_cols=True, multi_index_mode="off"
    ):
        """Apply a Hatchet query to the Thicket object.

        Arguments:
            query_obj (AbstractQuery, Query, or CompoundQuery): the query, represented as by Query, CompoundQuery, or (for legacy support)
                a subclass of Hatchet's AbstractQuery
            squash (bool): if true, run Thicket.squash before returning the result of
                the query
            update_inc_cols (boolean, optional): if True, update inclusive columns when
                performing squash.

        Returns:
            (thicket): a new Thicket object containing the data that matches the query
        """
        local_query_obj = query_obj
        if isinstance(query_obj, list):
            local_query_obj = ObjectQuery(query_obj, multi_index_mode=multi_index_mode)
        elif isinstance(query_obj, str):
            local_query_obj = parse_string_dialect(
                query_obj, multi_index_mode=multi_index_mode
            )
        elif not is_hatchet_query(query_obj):
            raise TypeError(
                "Encountered unrecognized query type (expected Query, CompoundQuery, or AbstractQuery, got {})".format(
                    type(query_obj)
                )
            )
        dframe_copy = self.dataframe.copy()
        index_names = self.dataframe.index.names
        dframe_copy.reset_index(inplace=True)
        query = (
            local_query_obj
            if not is_old_style_query(query_obj)
            else local_query_obj._get_new_query()
        )
        query_matches = self.query_engine.apply(query, self.graph, self.dataframe)
        filtered_df = dframe_copy.loc[dframe_copy["node"].isin(query_matches)]
        if filtered_df.shape[0] == 0:
            raise EmptyQuery("The provided query would have produced an empty Thicket.")
        filtered_df.set_index(index_names, inplace=True)

        filtered_th = self.deepcopy()
        filtered_th.dataframe = filtered_df

        if squash:
            return filtered_th.squash(update_inc_cols=update_inc_cols)
        return filtered_th

    def query_stats(self, query_obj, squash=True, update_inc_cols=True):
        """Apply a Hatchet query to the Thicket object.

        Arguments:
            query_obj (AbstractQuery): the query, represented as by a subclass of
                Hatchet's AbstractQuery
            squash (bool): if true, run Thicket.squash before returning the result of
                the query
            update_inc_cols (boolean, optional): if True, update inclusive columns when
                performing squash.

        Returns:
            (thicket): a new Thicket object containing the data that matches the query
        """
        local_query_obj = query_obj
        if isinstance(query_obj, list):
            local_query_obj = ObjectQuery(query_obj, multi_index_mode="off")
        elif isinstance(query_obj, str):
            local_query_obj = parse_string_dialect(query_obj, multi_index_mode="off")
        elif not is_hatchet_query(query_obj):
            raise TypeError(
                "Encountered unrecognized query type (expected Query, CompoundQuery, or AbstractQuery, got {})".format(
                    type(query_obj)
                )
            )
        sframe_copy = self.statsframe.dataframe.copy()
        sf_index_names = self.statsframe.dataframe.index.names
        sframe_copy.reset_index(inplace=True)

        query = (
            local_query_obj
            if not is_old_style_query(query_obj)
            else local_query_obj._get_new_query()
        )
        query_matches = self.query_engine.apply(
            query, self.statsframe.graph, self.statsframe.dataframe
        )
        filtered_sf_df = sframe_copy.loc[sframe_copy["node"].isin(query_matches)]

        if filtered_sf_df.shape[0] == 0:
            raise EmptyQuery("The provided query would have produced an empty Thicket.")

        df_index_names = self.dataframe.index.names
        dframe_copy = self.dataframe.copy()
        dframe_copy.reset_index(inplace=True)

        filtered_df = dframe_copy[dframe_copy["node"].isin(query_matches)]
        filtered_df.set_index(df_index_names, inplace=True)

        filtered_sf_df.set_index(sf_index_names, inplace=True)

        filtered_th = Thicket(
            graph=self.graph,
            dataframe=filtered_df,
            exc_metrics=self.exc_metrics.copy(),
            inc_metrics=self.inc_metrics.copy(),
            metadata=self.metadata.copy(),
            profile=copy.copy(self.profile),
            profile_mapping=copy.copy(self.profile_mapping),
            statsframe=GraphFrame(graph=self.graph, dataframe=filtered_sf_df),
            statsframe_ops_cache=self.statsframe_ops_cache.copy(),
        )

        if squash:
            filtered_th = filtered_th.squash(
                update_inc_cols=update_inc_cols, new_statsframe=True
            )
            filtered_th.statsframe.graph = filtered_th.graph

            filtered_th.reapply_stats_operations()

        return filtered_th

    def reapply_stats_operations(self):
        """Reapply most recent stats operations."""

        for stats_func, arg_dict in self.statsframe_ops_cache.items():
            # This makes sure the data is comparable
            sortkey = lambda x: (  # noqa: E731
                x[0],
                tuple(sorted(x[1].items(), key=lambda y: y[0])),
            )
            for k, g in itertools.groupby(
                list(sorted(arg_dict.values(), key=sortkey)), key=sortkey
            ):
                arg = list(g)[0]
                stats_func(self, *arg[0], **arg[1])

            validate_nodes(self)

    def groupby(self, by):
        """Create sub-thickets based on unique values in metadata column(s).

        Arguments:
            by (mapping, function, label, pd.Grouper or list of such): Used to determine the groups for the groupby. See pandas.DataFrame.groupby() for more details.

        Returns:
            (list): list of (sub)thickets
        """
        if not self.metadata.empty:
            # group metadata table by unique values in a column
            sub_metadataframes = self.metadata.groupby(by, dropna=False)

            # dictionary of sub_thickets
            sub_thickets = {}

            # for all unique groups of metadata table
            for key, df in sub_metadataframes:
                # create a thicket copy
                sub_thicket = self.deepcopy()

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

                # clear the aggregated statistics table for current unique group
                sub_thicket.statsframe.dataframe = helpers._new_statsframe_df(
                    sub_thicket.dataframe
                )

                # add thicket to dictionary
                sub_thickets[key] = sub_thicket

                sub_thicket._sync_profile_components(sub_thicket.metadata)
                validate_profile(sub_thicket)
        else:
            raise EmptyMetadataTable(
                "The provided Thicket object has an empty metadata table."
            )

        return GroupBy(by, sub_thickets)

    def filter_stats(self, filter_function):
        """Filter thicket object based on a stats column.

        Propagate changes to the entire thicket object.

        Arguments:
            select_function (lambda function): filter to apply to the aggregated
                statistics table

        Returns:
            (thicket): new thicket object with applied filter function
        """
        # deepcopy thicket
        new_thicket = self.deepcopy()

        # filter aggregated statistics table
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
        new_thicket = new_thicket.squash(new_statsframe=False)

        return new_thicket

    def move_metrics_to_statsframe(self, metric_columns, profile=None, override=False):
        if not isinstance(metric_columns, (list, tuple)):
            raise TypeError("'metric_columns' must be a list or tuple")
        profile_list = self.dataframe.index.unique(level="profile").tolist()
        if profile is None and len(profile_list) != 1:
            raise ValueError(
                "Cannot move a metric to statsframe when there are multiple profiles. Set the 'profile' argument to the profile you want to move"
            )
        if profile is not None and profile not in profile_list:
            raise ValueError("Invalid profile: {}".format(profile))
        df_for_profile = None
        if profile is None:
            df_for_profile = self.dataframe.reset_index(level="profile", drop=True)
        else:
            df_for_profile = self.dataframe.xs(
                profile, level="profile", drop_level=True
            )
        new_statsframe_df = self.statsframe.dataframe.copy(deep=True)
        for c in metric_columns:
            if c in new_statsframe_df and not override:
                raise KeyError(
                    "Column {} is already in statsframe. To replace the column, run with 'override=True'".format(
                        c
                    )
                )
            new_statsframe_df[c] = df_for_profile[c]
        self.statsframe.dataframe = new_statsframe_df
        verify_thicket_structures(
            self.statsframe.dataframe, columns=metric_columns, index=["node"]
        )

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

    def add_root_node(self, attrs):
        """Add node at root level with given attributes.

        Arguments:
            attrs (dict): attributes for the new node which will be used to initilize the
            node.frame.
        """

        new_node = Node(frame_obj=Frame(attrs=attrs))

        # graph and statsframe.graph
        self.graph.roots.append(new_node)

        # Set hatchet nid and depth
        self.graph.enumerate_traverse()

        # dataframe
        idx_levels = self.dataframe.index.names
        new_idx = [[new_node]] + [self.profile]
        new_node_df = pd.DataFrame(
            index=pd.MultiIndex.from_product(new_idx, names=idx_levels)
        )
        new_node_df["name"] = attrs["name"]
        self.dataframe = pd.concat([self.dataframe, new_node_df])

        # statsframe.dataframe
        self.statsframe.dataframe = helpers._new_statsframe_df(self.dataframe)
        # Reapply stats operations after clearing statsframe dataframe
        self.reapply_stats_operations()

        # Check Thicket state
        validate_nodes(self)

    def get_node(self, name):
        """Get a node object in the Thicket by its Node.frame['name']. If more than one
        node has the same name, a list of nodes is returned.

        Arguments:
            name (str): name of the node (Node.frame['name']).

        Returns:
            (Node or list(Node)): Node object with the given name or list of Node objects
            with the given name.
        """
        node = [n for n in self.graph.traverse() if n.frame["name"] == name]

        if len(node) == 0:
            raise KeyError(f'Node with name "{name}" not found.')

        return node[0] if len(node) == 1 else node

    def _sync_profile_components(self, component):
        """Synchronize the Performance DataFrame, Metadata Dataframe, profile and
        profile mapping objects based on the component's index or a list of profiles.
        This is useful when a non-Thicket function modifies the profiles in an object
        and those changes need to be reflected in the other objects.

        Arguments:
            component (list or DataFrame) -> (list, Thicket.dataframe, or Thicket.metadata): The index
            of this component is used to synchronize the other objects.
        """

        def _profile_truth_from_component(component):
            """Derive the profiles from the component index."""
            if isinstance(component, list):
                return component
            # Option A: Columnar-indexed Thicket
            elif isinstance(component.columns, pd.MultiIndex):
                # Performance DataFrame
                if isinstance(component.index, pd.MultiIndex):
                    row_idx = component.index.droplevel(level="node")
                # Metadata DataFrame
                else:
                    row_idx = component.index
                profile_truth = [
                    prof
                    for prof in self.profile
                    if any([row_prof in prof for row_prof in row_idx])
                ]
            # Option B: Non-columnar-indexed Thicket
            else:
                # Performance DataFrame
                if isinstance(component.index, pd.MultiIndex):
                    profile_truth = component.index.droplevel(level="node")
                # Metadata DataFrame
                else:
                    profile_truth = component.index
            return list(set(profile_truth))

        def _sync_indices(profile_truth):
            """Sync the Thicket attributes"""
            self.profile = profile_truth
            self.profile_mapping = OrderedDict(
                {
                    prof: file
                    for prof, file in self.profile_mapping.items()
                    if prof in profile_truth
                }
            )

            # For Columnar-indexed Thicket
            if isinstance(self.dataframe.columns, pd.MultiIndex):
                # Create powerset from all profiles
                pset = set()
                for p in profile_truth:
                    pset.update(helpers._powerset_from_tuple(p))
                profile_truth = list(pset)

            self.dataframe = self.dataframe[
                self.dataframe.index.droplevel(level="node").isin(profile_truth)
            ]
            self.metadata = self.metadata[self.metadata.index.isin(profile_truth)]

            return self

        if isinstance(component, list) or isinstance(component, pd.DataFrame):
            profile_truth = _profile_truth_from_component(component)
            self = _sync_indices(profile_truth)
        else:
            raise ValueError(
                "Component must be either list, Thicket.dataframe, or Thicket.metadata"
            )


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
