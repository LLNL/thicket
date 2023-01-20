# Copyright 2022 Lawrence Livermore National Security, LLC and other
# Thicket Project Developers. See the top-level LICENSE file for details.
#
# SPDX-License-Identifier: MIT


def verify_thicket_structures(thicket_component, columns=[], index=[]):
    """Assertion for missing input requirements to execute thicket functions.

    Arguments:
        thicket_components (DataFrame): component of thicket to check
        columns (list): list of columns to check for in the component
        index (list): list of index levels to check for in the component

    Returns:
        Raises an error if any columns or index levels are not in component,
        continues program if all columns and index levels are in component
    """
    # collect component columns and index
    component_columns = thicket_component.columns.tolist()
    component_index = thicket_component.index.names

    # check existence of columns and index
    column_result = all(elem in component_columns for elem in columns)
    index_result = all(elem in component_index for elem in index)

    # store missing collumns or index if they exist
    missing_columns = list(set(columns).difference(component_columns))
    missing_index = list(set(index).difference(component_index))

    if not column_result and not index_result:
        raise RuntimeError(
            "\n Missing column(s): "
            + missing_columns
            + " required for the function. \n Missing index level(s): "
            + missing_index
            + " required for the function"
        )
    elif not column_result and index_result:
        raise RuntimeError(
            "\n Missing column(s): " + missing_columns + " required for the function"
        )
    elif column_result and not index_result:
        raise RuntimeError(
            "\n Missing index level(s): " + missing_index + " required for the function"
        )
