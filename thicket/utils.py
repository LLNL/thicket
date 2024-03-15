# Copyright 2022 Lawrence Livermore National Security, LLC and other
# Thicket Project Developers. See the top-level LICENSE file for details.
#
# SPDX-License-Identifier: MIT

from collections import OrderedDict

import pandas as pd

from thicket import helpers


def check_same_frame(n1, n2):
    if n1.frame != n2.frame:
        raise ValueError(
            f"Cannot replace the node. Frames do not match: {n1.frame} != {n2.frame}"
        )


def validate_dataframe(df):
    """Check validity of a Thicket DataFrame."""

    def _check_duplicate_inner_idx(df):
        """Check for duplicate values in the innermost index."""
        for node in set(df.index.get_level_values("node")):
            inner_idx_values = sorted(
                df.loc[node].index.get_level_values(df.index.nlevels - 2).tolist()
            )
            inner_idx_values_set = sorted(list(set(inner_idx_values)))
            if inner_idx_values != inner_idx_values_set:
                raise IndexError(
                    f"The Thicket.dataframe's index has duplicate values. {inner_idx_values}"
                )

    def _check_missing_hnid(df):
        """Check if there are missing hatchet nid's."""
        i = 0
        set_of_nodes = set(df.index.get_level_values("node"))
        for node in set_of_nodes:
            if hash(node) != i:
                raise ValueError(
                    f"The Thicket.dataframe's index is either not sorted or has a missing node. {hash(node)} ({node}) != {i}"
                )
            i += 1

    def _validate_name_column(df):
        """Check if all of the values in a node's name column are either its name or None."""
        for node in set(df.index.get_level_values("node")):
            names = set(df.loc[node]["name"].tolist())
            node_name = node.frame["name"]
            for name in names:
                if name != node_name and name is not None:
                    raise ValueError(
                        f"Value in the Thicket.dataframe's 'name' column is not valid. {name} != {node_name}"
                    )

    _check_duplicate_inner_idx(df)
    _check_missing_hnid(df)
    _validate_name_column(df)


def validate_profile(tk):
    """Check validity of Thicket objects that rely on profiles. Thicket.dataframe, Thicket.metadata, Thicket.profile, Thicket.profile_mapping."""

    def _validate_multiindex_column(tk):
        """Check if the Thicket.dataframe and Thicket.metadata have the same column structure if MultiIndex present in either. Returns True if valid, raises ValueError if not valid."""
        if isinstance(tk.dataframe.columns, pd.MultiIndex) or isinstance(
            tk.metadata.columns, pd.MultiIndex
        ):
            if not isinstance(tk.dataframe.columns, pd.MultiIndex) or not isinstance(
                tk.metadata.columns, pd.MultiIndex
            ):
                raise ValueError(
                    "The Thicket.dataframe and Thicket.metadata must both have a MultiIndex column structure if MultiIndex present in either."
                )
        return True

    def _validate_all_same(tk):
        df_profs = set(tk.dataframe.index.droplevel(level="node"))
        meta_profs = set(tk.metadata.index)
        profs = set(tk.profile)
        pm_profs = set(tk.profile_mapping.keys())

        if isinstance(tk.dataframe.columns, pd.MultiIndex) and isinstance(
            tk.metadata.columns, pd.MultiIndex
        ):
            # Set of profiles that exist in the dataframe index
            df_profs = set(
                p
                for p in profs
                if any(dfp in helpers._powerset_from_tuple(p) for dfp in df_profs)
            )
            # Set of profiles that exist in the metadata index
            meta_profs = set(
                p
                for p in profs
                if any(mp in helpers._powerset_from_tuple(p) for mp in meta_profs)
            )

        if not df_profs == meta_profs == profs == pm_profs:
            raise ValueError(
                f"Profiles in the Thicket.dataframe, Thicket.metadata, Thicket.profile, and Thicket.profile_mapping are not the same. {df_profs} != {meta_profs} != {profs} != {pm_profs}."
            )

    _validate_multiindex_column(tk)
    _validate_all_same(tk)


def verify_sorted_profile(thicket_component):
    """Assertion to check if profiles are sorted in a thicket dataframe

    Arguments:
        thicket_component (DataFrame): component of thicket to check
    """
    profile_index_values = list(
        OrderedDict.fromkeys(
            thicket_component.index.get_level_values(
                thicket_component.index.nlevels - 1
            )  # Innermost index
        )
    )
    if profile_index_values != sorted(profile_index_values):
        raise ValueError(
            "The profiles in this dataframe must be sorted. Try 'pandas.DataFrame.sort_index'."
        )


def verify_thicket_structures(thicket_component, columns=[], index=[]):
    """Assertion for missing input requirements to execute thicket functions.

    Arguments:
        thicket_components (DataFrame): component of thicket to check
        columns (list): list of columns to check for in the component
        index (list): list of index levels to check for in the component

    Returns:
        Raises an error if any columns or index levels are not in component, continues
            program if all columns and index levels are in component
    """
    if not isinstance(columns, list):
        raise RuntimeError("columns= must be specified as a list")
    if not isinstance(index, list):
        raise RuntimeError("index= must be specified as a list")

    # collect component columns and index
    component_columns = thicket_component.columns.tolist()
    component_index = thicket_component.index.names

    # check existence of columns and index
    column_result = all(elem in component_columns for elem in columns)
    index_result = all(elem in component_index for elem in index)

    # store missing columns or index if they exist
    missing_columns = list(set(columns).difference(component_columns))
    missing_index = list(set(index).difference(component_index))

    if not column_result and not index_result:
        raise RuntimeError(
            "Specified column(s) not found: "
            + str(missing_columns)
            + "\nMissing index level(s): "
            + str(missing_index)
            + " required for the function"
        )
    elif not column_result and index_result:
        raise RuntimeError("Specified column(s) not found: " + str(missing_columns))
    elif column_result and not index_result:
        raise RuntimeError(
            "Missing index level(s): "
            + str(missing_index)
            + " required for the function"
        )
