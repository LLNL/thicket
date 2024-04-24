# Copyright 2022 Lawrence Livermore National Security, LLC and other
# Thicket Project Developers. See the top-level LICENSE file for details.
#
# SPDX-License-Identifier: MIT

# make flake8 unused names in this file.
# flake8: noqa: F401

from hatchet.query import (
    # New style queries
    # #################
    #
    # Core query types
    Query,
    ObjectQuery,
    StringQuery,
    parse_string_dialect,
    # Compound queries
    CompoundQuery,
    ConjunctionQuery,
    DisjunctionQuery,
    ExclusiveDisjunctionQuery,
    NegationQuery,
    # Errors
    InvalidQueryPath,
    InvalidQueryFilter,
    RedundantQueryFilterWarning,
    BadNumberNaryQueryArgs,
    #
    # Old style queries
    # #################
    AbstractQuery,
    NaryQuery,
    AndQuery,
    IntersectionQuery,
    OrQuery,
    UnionQuery,
    XorQuery,
    SymDifferenceQuery,
    NotQuery,
    QueryMatcher,
    CypherQuery,
    parse_cypher_query,
    is_hatchet_query,
)

__all__ = [
    "Query",
    "ObjectQuery",
    "StringQuery",
    "parse_string_dialect",
    "CompoundQuery",
    "ConjunctionQuery",
    "DisjunctionQuery",
    "ExclusiveDisjunctionQuery",
    "NegationQuery",
    "InvalidQueryPath",
    "InvalidQueryFilter",
    "RedundantQueryFilterWarning",
    "BadNumberNaryQueryArgs",
    "is_hatchet_query",
]


def is_new_style_query(query_obj):
    return issubclass(type(query_obj), Query) or issubclass(
        type(query_obj), CompoundQuery
    )


def is_old_style_query(query_obj):
    return issubclass(type(query_obj), AbstractQuery)
