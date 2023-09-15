# Copyright 2022 Lawrence Livermore National Security, LLC and other
# Thicket Project Developers. See the top-level LICENSE file for details.
#
# SPDX-License-Identifier: MIT

from collections import defaultdict


class GroupBy(dict):
    def __init__(self, by=None, *args, **kwargs):
        super(GroupBy, self).__init__(*args, **kwargs)
        self.by = by

    def agg(self, func):
        """Aggregate the Thickets' PerfData numerical columns in a GroupBy object.

        Arguments:
            func (dict): Dictionary mapping from {str: function}, where the str will be added to the Thicket's column's name after the aggregation function is applied.

        Returns:
            (Thicket): Aggregated Thicket object.
        """
        agg_tks = {}
        for k, v in self.items():
            agg_tks[k] = self.aggregate_thicket(tk=v, func=func)

        values_list = list(agg_tks.values())
        first_tk = values_list[0]  # TODO: Hack to avoid circular import.
        agg_tk = first_tk.concat_thickets(values_list)

        return agg_tk

    def aggregate_thicket(self, tk, func):
        """Aggregate a Thicket's numerical columns given a statistical function.

        Arguments:
            tk (Thicket): Thicket object to aggregate.
            func (dict): See agg()

        Returns:
            (Thicket): New Thicket object with aggregated attributes.
        """

        def rename_col(total_cols, tname):
            """Helper function for renaming columns"""
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

        # Set perf_indices
        perf_indices = ["node"]
        if self.by is not None:
            other_indices = self.by
            if isinstance(self.by, str):
                other_indices = [other_indices]
            perf_indices.extend(other_indices)
        # agg_cols is all numeric columns
        agg_cols = list(tk.dataframe.select_dtypes(include="number").columns)
        # other_cols is agg_cols complement
        other_cols = list(tk.dataframe.select_dtypes(exclude="number").columns)

        # Get perf_indices into index
        index_names = tk_c.dataframe.index.names
        df_columns = tk_c.dataframe.columns
        for col in perf_indices:
            if col not in index_names:
                if col in tk_c.metadata.columns or col in df_columns:
                    if col not in df_columns:
                        tk_c.add_column_from_metadata_to_ensemble(col)
                    tk_c.dataframe = tk_c.dataframe.set_index(col, append=True)
                else:
                    raise KeyError(f'"{col}" is not in the PerfData or MetaData.')

        # Make new profile and profile_mapping
        new_profile_label_mapping_df = (
            tk_c.dataframe.reset_index()
            .drop(list(tk_c.dataframe.columns) + ["node"], axis=1)
            .drop_duplicates()
            .set_index("profile")
        )
        if (
            len(new_profile_label_mapping_df.columns) > 1
        ):  # Make tuple values if more than 1 column
            new_profile_label_mapping_df = new_profile_label_mapping_df.apply(
                tuple, axis=1
            )
        else:  # Squeeze single column df into series
            new_profile_label_mapping_df = new_profile_label_mapping_df.squeeze()
        new_profile_label_mapping = (
            new_profile_label_mapping_df.to_dict()
        )  # Convert df to dict
        new_profile = list(set(new_profile_label_mapping.values()))
        new_profile_mapping = defaultdict(list)
        for p in tk_c.profile:
            new_profile_mapping[new_profile_label_mapping[p]].append(
                tk_c.profile_mapping[p]
            )
        tk_c.profile = new_profile
        tk_c.profile_mapping = new_profile_mapping
        # Aggregate metadata
        tk_c.metadata = tk_c.metadata.reset_index()
        tk_c.metadata["profile"] = tk_c.metadata["profile"].map(
            new_profile_label_mapping
        )
        tk_c.metadata = tk_c.metadata.set_index("profile")
        tk_c.metadata = tk_c.metadata.groupby("profile").agg(_agg_rows)

        # Compute stats
        snames = list(func.keys())
        sfuncs = list(func.values())
        agg_df = tk_c.dataframe[agg_cols].groupby(perf_indices).agg(sfuncs[0])
        agg_df = agg_df.rename(columns=rename_col(agg_cols, snames[0]))
        for i in range(1, len(func)):
            t_agg_df = tk_c.dataframe[agg_cols].groupby(perf_indices).agg(sfuncs[i])
            t_agg_df = t_agg_df.rename(columns=rename_col(agg_cols, snames[i]))
            agg_df = agg_df.merge(
                right=t_agg_df,
                on=perf_indices,
            )

        # Create new df with other columns
        all_cols = perf_indices.copy()
        all_cols.extend(other_cols)
        other_df = (
            tk_c.dataframe.reset_index()[all_cols]
            .drop_duplicates(all_cols)
            .set_index(perf_indices)
        )
        agg_df = agg_df.merge(right=other_df, how="left", on=perf_indices)

        # Set other attributes
        tk_c.dataframe = agg_df

        return tk_c
