from tests import InternalTestCase
from transpark.operations.joins import (
    left_anti_join_df,
    left_join_df,
    right_join_df,
    inner_join_df,
    Condition,
)
from unittest import mock
from transpark.utils.mappings import SupportedJoin
from parameterized import parameterized, param
from typing import Callable
from pyspark.sql import DataFrame


class JoinTestCase(InternalTestCase):

    @parameterized.expand(
        [
            param(
                "__left_join_default_alias",
                alias=["l", "r"],
                join_type=SupportedJoin.LEFT.value,
                func=left_join_df,
            ),
            param(
                "__left_join_custom_alias",
                alias=["left", "right"],
                join_type=SupportedJoin.LEFT.value,
                func=left_join_df,
            ),
            param(
                "__right_join_default_alias",
                alias=["l", "r"],
                join_type=SupportedJoin.RIGHT.value,
                func=right_join_df,
            ),
            param(
                "__right_join_custom_alias",
                alias=["left", "right"],
                join_type=SupportedJoin.RIGHT.value,
                func=right_join_df,
            ),
            param(
                "__inner_join_default_alias",
                alias=["l", "r"],
                join_type=SupportedJoin.INNER.value,
                func=inner_join_df,
            ),
            param(
                "__inner_join_custom_alias",
                alias=["left", "right"],
                join_type=SupportedJoin.INNER.value,
                func=inner_join_df,
            ),
            param(
                "__left_anti_join_default_alias",
                alias=["l", "r"],
                join_type=SupportedJoin.LEFT_ANTI.value,
                func=left_anti_join_df,
            ),
            param(
                "__left__anti_join_custom_alias",
                alias=["left", "right"],
                join_type=SupportedJoin.LEFT_ANTI.value,
                func=left_anti_join_df,
            ),
        ]
    )
    def test__join__works_fine(self, _, alias, join_type, func):
        func: Callable[[DataFrame, DataFrame, Condition, str, str]] = func

        df_left = mock.MagicMock()
        df_right = mock.MagicMock()

        # Chain alias to return the same mock so we can test join
        df_left.alias.return_value = df_left
        df_right.alias.return_value = df_right
        df_left.join.return_value = mock.MagicMock()

        l_alias, r_alias = alias

        func(
            left=df_left,
            right=df_right,
            cond=["col1", "col2"],
            alias_left=l_alias,
            alias_right=r_alias,
        )

        df_left.alias.assert_called_with(l_alias)
        df_right.alias.assert_called_with(r_alias)
        df_left.join.assert_called_once()
        df_left.join.assert_called_once_with(
            df_right.alias.return_value,
            on=[
                f"{l_alias}.col1 == {r_alias}.col1",
                f"{l_alias}.col2 == {r_alias}.col2",
            ],
            how=join_type,
        )
