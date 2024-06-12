from functools import wraps


def cache_stats_op(func):
    """Python decorator that handles insertion of stats operations in the thicket statsframe_ops_cache."""

    @wraps(func)
    def wrapper(thicket, *args, **kwargs):
        output_columns = func(thicket, *args, **kwargs)
        if func not in thicket.statsframe_ops_cache:
            thicket.statsframe_ops_cache[func] = {}

        for column in output_columns:
            thicket.statsframe_ops_cache[func][column] = (args, kwargs)
        return output_columns

    return wrapper
