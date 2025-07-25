from pyspark.sql import DataFrame
from collections.abc import Sequence
from transpark.utils.mappings import SupportedJoin, JoinCondition

# A condition may be:
# - a single JoinCondition
# - a list of JoinCondition objects
# - a list of column names (str)
Condition = Sequence[str | JoinCondition | Sequence[JoinCondition]]


def __join_base(
    left: DataFrame,
    right: DataFrame,
    cond: Condition,
    join_type: SupportedJoin,
    alias_left: str = "l",
    alias_right: str = "r",
) -> DataFrame:
    """
    Internal function to perform a join between two DataFrames.

    Args:
        left (DataFrame): The left DataFrame.
        right (DataFrame): The right DataFrame.
        cond (Condition): The join condition, which can be:
            - a single JoinCondition
            - a list of JoinCondition objects
            - a list of column names (for equality joins)
        join_type (SupportedJoin): Type of join (e.g., LEFT, RIGHT, INNER).
        alias_left (str): Alias for the left DataFrame (default "l").
        alias_right (str): Alias for the right DataFrame (default "r").

    Returns:
        DataFrame: Result of the join operation.

    Raises:
        ValueError: If the join condition format is unsupported.
    """
    match cond:
        case JoinCondition() as c:
            _cond = [f"{alias_left}{c.left_col} {c.sign} {alias_right}{c.right_col}"]

        case list() as cc if all(isinstance(c, JoinCondition) for c in cond):
            _cond = [
                f"{alias_left}{c.left_col} {c.sign} {alias_right}{c.right_col}"
                for c in cc
            ]

        case list() as cc if all(isinstance(c, str) for c in cond):
            _cond = [f"{alias_left}.{col} == {alias_right}.{col}" for col in cc]

        case _:
            raise ValueError("Unsupported join condition type")

    return left.alias(alias_left).join(
        right.alias(alias_right), on=_cond, how=join_type.value
    )


def left_join_df(
    left: DataFrame,
    right: DataFrame,
    cond: Condition,
    alias_left: str = "l",
    alias_right: str = "r",
) -> DataFrame:
    """
    Perform a left join between two DataFrames.

    See `__join_base` for parameter details.
    """
    return __join_base(
        left=left,
        right=right,
        cond=cond,
        join_type=SupportedJoin.LEFT,
        alias_left=alias_left,
        alias_right=alias_right,
    )


def right_join_df(
    left: DataFrame,
    right: DataFrame,
    cond: Condition,
    alias_left: str = "l",
    alias_right: str = "r",
) -> DataFrame:
    """
    Perform a right join between two DataFrames.

    See `__join_base` for parameter details.
    """
    return __join_base(
        left=left,
        right=right,
        cond=cond,
        join_type=SupportedJoin.RIGHT,
        alias_left=alias_left,
        alias_right=alias_right,
    )


def inner_join_df(
    left: DataFrame,
    right: DataFrame,
    cond: Condition,
    alias_left: str = "l",
    alias_right: str = "r",
) -> DataFrame:
    """
    Perform an inner join between two DataFrames.

    See `__join_base` for parameter details.
    """
    return __join_base(
        left=left,
        right=right,
        cond=cond,
        join_type=SupportedJoin.INNER,
        alias_left=alias_left,
        alias_right=alias_right,
    )


def left_anti_join_df(
    left: DataFrame,
    right: DataFrame,
    cond: Condition,
    alias_left: str = "l",
    alias_right: str = "r",
) -> DataFrame:
    """
    Perform a left anti join between two DataFrames.

    See `__join_base` for parameter details.
    """
    return __join_base(
        left=left,
        right=right,
        cond=cond,
        join_type=SupportedJoin.LEFT_ANTI,
        alias_left=alias_left,
        alias_right=alias_right,
    )
