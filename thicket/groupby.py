# Copyright 2022 Lawrence Livermore National Security, LLC and other
# Thicket Project Developers. See the top-level LICENSE file for details.
#
# SPDX-License-Identifier: MIT

from collections import defaultdict
from pandas.api.types import is_numeric_dtype, is_string_dtype


class GroupBy(dict):
    def __init__(self, *args, **kwargs):
        super(GroupBy, self).__init__(*args, **kwargs)

    def agg(self, func, gb_col=None, agg_cols=None, aux_cols=None):
        """Aggregate the Thickets in a GroupBy object.

        Arguments:
            func (dict): Mapping from str -> function, where the str will be added to the Thicket's column's name after the aggregation function is applied.
            gb_cols (str, optional): Optional column to group on in addition to "node". can be from PerfData or MetaData. Default grouping is on "node".
            gb_cols (list(str), optional): Specify which numerical columns to aggregate.
            aux_cols (list(str), optional): Additional existing columns from the previous Thicket.dataframe to pull into the new aggregated dataframe.

        Returns:
            (self): Aggregated GroupBy object.
        """
        for k, v in self.items():
            self[k] = GroupBy.aggregate_thicket(
                tk=v, func=func, gb_col=gb_col, agg_cols=agg_cols, aux_cols=aux_cols
            )
        return self

    @staticmethod
    def aggregate_thicket(tk, func, gb_col=None, agg_cols=None, aux_cols=None):
        """Aggregate a Thicket's numerical columns given a statistical function.

        Arguments:
            tk (Thicket): Thicket object to aggregate.
            func (dict): See agg()
            gb_cols(str, optional):: See agg()
            aux_cols (list(str), optional): See agg()

        Returns:
            (Thicket): New Thicket object with aggregated attributes.
        """

        def rename_col(total_cols, tname):
            tcols = {}
            for col in total_cols:
                tcols[col] = col + "_" + tname
            return tcols

        def _agg_rows(col_series):
            """Aggregate values in 'col_series' into a set to remove duplicates."""
            if len(col_series) <= 1:
                return col_series
            else:
                if isinstance(col_series.iloc[0], list) or isinstance(
                    col_series.iloc[0], set
                ):
                    _set = set(tuple(i) for i in col_series)
                else:
                    _set = set(col_series)
                if len(_set) == 1:
                    return _set.pop()
                else:
                    return _set

        # Make copy
        tk_c = tk.deepcopy()

        # Set variables
        if gb_col:
            new_profile_idx = gb_col
            gb_cols = ["node", gb_col]
        else:
            new_profile_idx = "profile"
            gb_cols = ["node"]
        if not aux_cols:
            aux_cols = []
            for col in tk.dataframe.columns:
                if is_string_dtype(tk.dataframe[col]):
                    aux_cols.append(col)

        # Get gb_cols into index
        index_names = tk_c.dataframe.index.names
        df_columns = tk_c.dataframe.columns
        for col in gb_cols:
            if col not in index_names:
                if col in tk_c.metadata.columns or col in df_columns:
                    if col not in df_columns:
                        tk_c.add_column_from_metadata_to_ensemble(col)
                    tk_c.dataframe = tk_c.dataframe.set_index(col, append=True)
                else:
                    raise KeyError(f'"{col}" is not in the PerfData or MetaData.')

        # agg_cols is all numeric columns
        if not agg_cols:
            agg_cols = []
            for col in tk.dataframe.columns:
                if is_numeric_dtype(tk.dataframe[col]):
                    agg_cols.append(col)

        # Compute stats
        snames = list(func.keys())
        sfuncs = list(func.values())
        agg_df = tk_c.dataframe[agg_cols].groupby(gb_cols).agg(sfuncs[0])
        agg_df = agg_df.rename(columns=rename_col(agg_cols, snames[0]))
        for i in range(1, len(func)):
            t_agg_df = tk_c.dataframe[agg_cols].groupby(gb_cols).agg(sfuncs[i])
            t_agg_df = t_agg_df.rename(columns=rename_col(agg_cols, snames[i]))
            agg_df = agg_df.merge(
                right=t_agg_df,
                on=gb_cols,
            )

        # Create new df with auxilliary columns
        all_cols = gb_cols.copy()
        all_cols.extend(aux_cols)
        aux_df = (
            tk_c.dataframe.reset_index()[all_cols]
            .drop_duplicates(all_cols)
            .set_index(gb_cols)
        )
        agg_df = agg_df.merge(right=aux_df, how="left", on=gb_cols)

        # Create profile and profile mapping
        if new_profile_idx == "profile":
            new_profiles = [0]
            new_profile_mapping = defaultdict(list)
            new_profile_mapping[0] = list(tk_c.profile_mapping.keys())
            # Aggregate metadata dataframe
            tk_c.metadata = tk_c.metadata.reset_index(drop=True)
            tk_c.metadata["profile"] = 0
            tk_c.metadata = tk_c.metadata.set_index("profile")
            tk_c.metadata = tk_c.metadata.groupby("profile").agg(_agg_rows)
        else:
            mapping_to_new_profiles = (
                tk_c.dataframe.reset_index()[["profile", new_profile_idx]]
                .drop_duplicates()
                .set_index("profile")
                .to_dict()[new_profile_idx]
            )
            new_profile_mapping = defaultdict(list)
            for k, v in mapping_to_new_profiles.items():
                new_profile_mapping[v].append(tk_c.profile_mapping[k])
            new_profiles = list(new_profile_mapping.keys())
            # Aggregate metadata dataframe
            tk_c.metadata = tk_c.metadata.groupby(new_profile_idx).agg(_agg_rows)

        # Set other attributes
        tk_c.dataframe = agg_df
        tk_c.profile = new_profiles
        tk_c.profile_mapping = new_profile_mapping

        return tk_c
