import pytest
import re

import hatchet as ht
from thicket import Thicket, InvalidFilter, EmptyMetadataFrame


def test_filter(example_cali):
    # example thicket
    th = Thicket.from_caliperreader(example_cali)

    # columns and corresponding values to filter by
    columns_values = {"problem_size": ["30"], "cluster": ["quartz", "chekov"]}

    for column in columns_values:
        for value in columns_values[column]:
            # get expected profile hash and nodes after filtering
            exp_profile = sorted(
                th.metadata.index[th.metadata[column] == value].tolist()
            )
            exp_nodes = sorted(
                th.dataframe[
                    th.dataframe.index.get_level_values("profile").isin(exp_profile)
                ]
                .index.get_level_values(0)
                .drop_duplicates()
                .tolist()
            )

            # filter function
            new_th = th.filter_metadata(lambda x: x[column] == value)

            # check if output is a thicket object
            assert isinstance(new_th, Thicket)

            # MetadataFrame: compare profile hash keys after filter to expected
            metadata_profile = new_th.metadata.index.tolist()
            assert metadata_profile == exp_profile

            # EnsembleFrame: compare profile hash keys and nodes after filter to expected
            ensemble_profile = sorted(
                new_th.dataframe.index.get_level_values(1).drop_duplicates().tolist()
            )
            ensemble_nodes = sorted(
                new_th.dataframe.index.get_level_values(0).drop_duplicates().tolist()
            )
            assert ensemble_profile == exp_profile
            assert ensemble_nodes == exp_nodes

            # StatsFrame: compare nodes after filter to expected; check for empty dataframe
            stats_nodes = sorted(
                new_th.statsframe.dataframe.index.get_level_values(0)
                .drop_duplicates()
                .tolist()
            )
            assert exp_nodes == stats_nodes
            assert "name" in new_th.statsframe.dataframe.columns

    # check for invalid filter exception
    with pytest.raises(InvalidFilter):
        th.filter_metadata(123)

    # drop all rows of the metadataframe
    th.metadata = th.metadata.iloc[0:0]

    # check for empty metadataframe exception
    with pytest.raises(EmptyMetadataFrame):
        th.filter_metadata(lambda x: x["cluster"] == "chekov")


def test_filter_stats(example_cali):
    # example thicket
    th = Thicket.from_caliperreader(example_cali)

    # columns and corresponding values to filter by
    columns_values = {
        "test_string_column": ["less than 20"],
        "test_numeric_column": [4, 15],
    }

    # set string column values
    less_than_20 = ["less than 20"] * 21
    less_than_45 = ["less than 45"] * 25
    less_than_87 = ["less than 87"] * 40
    new_col = less_than_20 + less_than_45 + less_than_87

    th.statsframe.dataframe["test_string_column"] = new_col

    # set numeric column values
    th.statsframe.dataframe["test_numeric_column"] = range(0, 86)

    for column in columns_values:
        for value in columns_values[column]:
            # for type str column
            if type(value) == str:
                # expected nodes after applying filter
                exp_nodes = sorted(
                    th.statsframe.dataframe.index[
                        th.statsframe.dataframe[column] == value
                    ]
                )
                new_th = th.filter_stats(lambda x: x[column] == value)
            # for type int column
            elif type(value) == int:
                exp_nodes = sorted(
                    th.statsframe.dataframe.index[
                        th.statsframe.dataframe[column] < value
                    ]
                )
                new_th = th.filter_stats(lambda x: x[column] < value)
            else:
                # test case not implemented
                print("The column value type is not a supported test case")
                exp_nodes = []
                new_th = th

            # check if output is a thicket object
            assert isinstance(new_th, Thicket)

            # fitlered statsframe nodes
            stats_nodes = sorted(
                new_th.statsframe.dataframe.index.drop_duplicates().tolist()
            )
            # check filtered statsframe nodes match exp_nodes
            assert stats_nodes == exp_nodes

            # filtered ensemble nodes
            ensemble_nodes = sorted(
                new_th.dataframe.index.get_level_values(0).drop_duplicates().tolist()
            )
            # check filtered ensembleframe nodes match exp_nodes
            assert ensemble_nodes == exp_nodes


def test_groupby(example_cali):
    # example thicket
    th = Thicket.from_caliperreader(example_cali)
    # use cases for string, numeric, and single value columns
    columns = ["user", "launchdate", "cali.channel"]

    # inspect all use cases
    for col_value in columns:
        unique_values = sorted(th.metadata[col_value].unique().tolist())
        th_list = th.groupby(col_value)
        # inspect all unique values in the use case
        for itr, uni_val in enumerate(unique_values):
            # expected profile hashes and nodes
            exp_profiles = sorted(
                th.metadata.index[th.metadata[col_value] == uni_val].tolist()
            )
            exp_nodes = sorted(
                th.dataframe[th.dataframe.index.get_level_values(1).isin(exp_profiles)]
                .index.get_level_values(0)
                .drop_duplicates()
                .tolist()
            )

            # metadata profile hash for unique value sub-thicket
            metadata_profiles = sorted(th_list[itr].metadata.index.tolist())
            assert metadata_profiles == exp_profiles

            # ensemble profile hash for unique value sub-thicket
            ensemble_profiles = sorted(
                th_list[itr]
                .dataframe.index.get_level_values(1)
                .drop_duplicates()
                .tolist()
            )
            assert ensemble_profiles == exp_profiles

            # ensemble nodes for unique value sub-thicket
            ensemble_nodes = sorted(
                th_list[itr]
                .dataframe.index.get_level_values(0)
                .drop_duplicates()
                .tolist()
            )
            assert ensemble_nodes == exp_nodes

            # stats nodes for unique value sub-thicket
            stats_nodes = sorted(
                th_list[itr]
                .statsframe.dataframe.index.get_level_values(0)
                .drop_duplicates()
                .tolist()
            )
            assert stats_nodes == exp_nodes

            # check for name column statsframe dataframe
            assert "name" in th_list[itr].statsframe.dataframe.columns

    # drop all rows of the metadataframe
    th.metadata = th.metadata.iloc[0:0]

    # check for empty metadataframe exception
    with pytest.raises(EmptyMetadataFrame):
        th.groupby(["user"])


def test_query(rajaperf_basecuda_xl_cali):
    th = Thicket.from_caliperreader(rajaperf_basecuda_xl_cali)
    th_df_profiles = th.dataframe.index.get_level_values("profile")
    match = [
        node
        for node in th.graph.traverse()
        if node._hatchet_nid in [0, 1, 2, 3, 5, 6, 8, 9]
    ]
    match_frames = [node.frame for node in match]
    match_names = [frame["name"] for frame in match_frames]
    query = (
        ht.QueryMatcher()
        .match("*")
        .rel(
            ".",
            lambda row: row["name"]
            .apply(lambda x: re.match(r"Algorithm.*block_128", x) is not None)
            .all(),
        )
    )
    filt_th = th.query(query)
    filt_nodes = list(filt_th.graph.traverse())

    assert sorted(list(filt_th.dataframe.index.names)) == sorted(["node", "profile"])
    filt_th_df_nodes = filt_th.dataframe.index.get_level_values("node").to_list()
    filt_th_df_profiles = filt_th.dataframe.index.get_level_values("profile")

    assert len(filt_nodes) == len(match)
    assert all([n.frame in match_frames for n in filt_nodes])
    assert all([n.frame["name"] in match_names for n in filt_nodes])
    assert all([n in filt_th_df_nodes for n in filt_nodes])
    assert sorted(filt_th_df_profiles.unique().to_list()) == sorted(
        th_df_profiles.unique().to_list()
    )

    # update_inclusive_columns is called automatically by GraphFrame.squash()
    # at the end of "query".
    # To make sure the following assertion works, manually invoke this function
    # on the original data.
    # TODO if/when the call to update_inclusive_columns is made optional in Hatchet,
    #      remove this line
    th.update_inclusive_columns()
    assert sorted(list(filt_th.dataframe.columns)) == sorted(list(th.dataframe.columns))
