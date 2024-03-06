# Copyright 2022 Lawrence Livermore National Security, LLC and other
# Thicket Project Developers. See the top-level LICENSE file for details.
#
# SPDX-License-Identifier: MIT

from collections import OrderedDict


# Define custom errors for external checks
class DuplicateIndexError(IndexError):
    pass


class MissingHNIDError(ValueError):
    pass


class InvalidNameError(ValueError):
    pass


def check_same_frame(n1, n2):
    if n1.frame != n2.frame:
        raise ValueError(
            f"Cannot replace the node. Frames do not match: {n1.frame} != {n2.frame}"
        )


def validate_dataframe(df):
    """Check validity of a Thicket DataFrame."""

    def _check_duplicate_inner_idx(df):
        """Check for duplicate values in the innermost indices."""
        for node in set(df.index.get_level_values("node")):
            inner_idx_values = sorted(df.loc[node].index.tolist())
            inner_idx_values_set = sorted(list(set(inner_idx_values)))
            if inner_idx_values != inner_idx_values_set:
                raise DuplicateIndexError(
                    f"Duplicate index {set(inner_idx_values)} found in DataFrame index.\nCheck that each Thicket.dataframe.index is unique. If metadata_key provided, check that each key is unique. Multiple-trial data must be aggregated before using this function (see Thicket.groupby.agg())."
                )

    def _check_missing_hnid(df):
        """Check if there are missing hatchet nid's."""
        i = 0
        set_of_nodes = set(df.index.get_level_values("node"))
        for node in set_of_nodes:
            if hash(node) != i:
                raise MissingHNIDError(
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
                    raise InvalidNameError(
                        f"Value in the Thicket.dataframe's 'name' column is not valid. {name} != {node_name}"
                    )

    _check_duplicate_inner_idx(df)
    _check_missing_hnid(df)
    _validate_name_column(df)


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
