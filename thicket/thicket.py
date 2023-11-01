# Copyright 2022 Lawrence Livermore National Security, LLC and other
# Thicket Project Developers. See the top-level LICENSE file for details.
#
# SPDX-License-Identifier: MIT

import copy
import os
import sys
import json
import warnings
from collections import OrderedDict
from hashlib import md5

import pandas as pd
import numpy as np
from hatchet import GraphFrame
from hatchet.query import AbstractQuery, QueryMatcher

from thicket.ensemble import Ensemble
import thicket.helpers as helpers
from .utils import verify_thicket_structures
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
            # Store used profiles and profile mappings using a truncated md5 hash of their string.
            # Resulting int hash will be at least hex_length digits and theoretically up to
            # ceil(log_10(16^n - 1)) digits after conversion.

            # length of the hex string before being converted to an integer.
            hex_length = 8

            hash_arg = int(md5(prf.encode("utf-8")).hexdigest()[:hex_length], 16)
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
        extra_args = []
        if len(args) > 1:
            extra_args = args[1:]

        # Parse the input object
        # if a list of files
        if isinstance(obj, (list, tuple)):
            for file in obj:
                ens_list.append(
                    Thicket.thicketize_graphframe(
                        func(file, *extra_args, **kwargs), file
                    )
                )
        # if directory of files
        elif os.path.isdir(obj):
            for file in os.listdir(obj):
                f = os.path.join(obj, file)
                ens_list.append(
                    Thicket.thicketize_graphframe(func(f, *extra_args, **kwargs), f)
                )
        # if single file
        elif os.path.isfile(obj):
            return Thicket.thicketize_graphframe(func(*args, **kwargs), args[0])
        # Error checking
        else:
            if isinstance(obj, str) and not os.path.isfile(obj):
                raise FileNotFoundError("File '" + obj + "' not found.")
            else:
                raise TypeError(
                    "'"
                    + str(type(obj).__name__)
                    + "' is not a valid type to be read from."
                )

        # Perform ensembling operation
        calltree = "union"
        if intersection:
            calltree = "intersection"
        thicket_object = Thicket.concat_thickets(
            thickets=ens_list,
            axis="index",
            calltree=calltree,
        )

        return thicket_object

    @staticmethod
    def concat_thickets(thickets, axis="index", calltree="union", **kwargs):
        """Concatenate thickets together on index or columns. The calltree can either be unioned or
        intersected which will affect the other structures.

        Arguments:
            thickets (list): list of thicket objects
            axis (str): axis to concatenate on -> "index" or "column"
            calltree (str): calltree to use -> "union" or "intersection"

            valid kwargs:
                if axis="index":
                    from_statsframes (bool): Whether this method was invoked from from_statsframes
                if axis="columns":
                    headers (list): List of headers to use for the new columnar multi-index.
                    metadata_key (str): Name of the column from the metadata tables to replace the 'profile'
                index. If no argument is provided, it is assumed that there is no profile-wise
                relationship between the thickets.

        Returns:
            (thicket): concatenated thicket
        """

        def _index(thickets, from_statsframes=False):
            thicket_parts = Ensemble._index(
                thickets=thickets, from_statsframes=from_statsframes
            )

            return Thicket(
                graph=thicket_parts[0],
                dataframe=thicket_parts[1],
                exc_metrics=thicket_parts[2],
                inc_metrics=thicket_parts[3],
                metadata=thicket_parts[4],
                profile=thicket_parts[5],
                profile_mapping=thicket_parts[6],
            )

        def _columns(thickets, headers=None, metadata_key=None):
            combined_thicket = Ensemble._columns(
                thickets=thickets, headers=headers, metadata_key=metadata_key
            )

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
        multiindex = False
        if isinstance(self.statsframe.dataframe.columns, pd.MultiIndex):
            multiindex = True
        stats_df = helpers._new_statsframe_df(new_dataframe, multiindex=multiindex)
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

        Returns:
            str: String representation of the tree, ready to print
        """
        color = sys.stdout.isatty()
        shell = None
        if metric_column is None:
            metric_column = self.default_metric

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

        return ThicketRenderer(unicode=unicode, color=color).render(
            self.graph.roots,
            self.statsframe.dataframe,
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
        )

    @staticmethod
    def from_statsframes(th_list, metadata_key=None):
        """Compose a list of Thickets with data in their statsframes.

        The Thicket's individual aggregated statistics tables are ensembled and become the
        new Thickets performance data table.

        Arguments:
            th_list (list): list of thickets
            metadata_key (str, optional): name of the metadata column to use as
                the new second-level index. Uses the first value so this only makes
                sense if provided column is all equal values and each thicket's columns
                differ in value.

        Returns:
            (thicket): New Thicket object.
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
        if metadata_key is None:
            for i in range(len(th_list)):
                th_names.append(i)
        else:  # metadata_key was provided.
            for th in th_list:
                # Get name from metadata table
                name_list = th.metadata[metadata_key].tolist()

                if len(name_list) > 1:
                    warnings.warn(
                        f"Multiple values for name {name_list} at thicket.metadata[{metadata_key}]. Only the first will be used."
                    )
                th_names.append(name_list[0])

        th_copy_list = []
        for i in range(len(th_list)):
            th_copy = th_list[i].deepcopy()

            th_id = th_names[i]

            if metadata_key is None:
                idx_name = "profile"
            else:
                idx_name = metadata_key

            # Modify graph
            # Necessary so node ids match up
            th_copy.graph = th_copy.statsframe.graph

            # Modify the performance data table
            df = th_copy.statsframe.dataframe
            df[idx_name] = th_id
            df.set_index(idx_name, inplace=True, append=True)
            th_copy.dataframe = df

            # Adjust profile and profile_mapping
            th_copy.profile = [th_id]
            profile_paths = list(th_copy.profile_mapping.values())
            th_copy.profile_mapping = OrderedDict({th_id: profile_paths})

            # Modify metadata dataframe
            th_copy.metadata[idx_name] = th_id
            th_copy.metadata.set_index(idx_name, inplace=True)

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
            th_copy.metadata = th_copy.metadata.groupby(idx_name).agg(_agg_to_set)

            # Append copy to list
            th_copy_list.append(th_copy)

        return Thicket.concat_thickets(th_copy_list, from_statsframes=True)

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

        # Row that didn't exist will contain "None" in the name column.
        query = (
            QueryMatcher()
            .match(".", lambda row: row["name"].apply(lambda n: n is not None).all())
            .rel("*")
        )
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

        filtered_th = self.deepcopy()
        filtered_th.dataframe = filtered_df

        if squash:
            return filtered_th.squash(update_inc_cols=update_inc_cols)
        return filtered_th

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

                # add thicket to dictionary
                sub_thickets[key] = sub_thicket
        else:
            raise EmptyMetadataTable(
                "The provided Thicket object has an empty metadata table."
            )

        print(len(sub_thickets), " thickets created...")
        print(sub_thickets)

        return sub_thickets

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
