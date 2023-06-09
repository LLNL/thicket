# Copyright 2022 Lawrence Livermore National Security, LLC and other
# Thicket Project Developers. See the top-level LICENSE file for details.
#
# SPDX-License-Identifier: MIT

from hatchet.query import (
    Query,
    ObjectQuery,
    StringQuery,
    parse_string_dialect,
    CompoundQuery,
    ConjunctionQuery,
    DisjunctionQuery,
    ExclusiveDisjunctionQuery,
    NegationQuery,
    QueryEngine,
    InvalidQueryPath,
    InvalidQueryFilter,
    RedundantQueryFilterWarning,
    BadNumberNaryQueryArgs,
)

from hatchet.query.compat import (
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
)

import hatchet.query.is_hatchet_query

def is_thicket_query(query_obj):
    return hatchet.query.is_hatchet_query(query_obj)


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
    "QueryEngine",
    "InvalidQueryPath",
    "InvalidQueryFilter",
    "RedundantQueryFilterWarning",
    "BadNumberNaryQueryArgs",
    "is_thicket_query",
]
