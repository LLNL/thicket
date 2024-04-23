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
