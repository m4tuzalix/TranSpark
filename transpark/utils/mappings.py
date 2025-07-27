from enum import StrEnum, auto
from dataclasses import dataclass
from typing import Literal, Optional, Callable, TypeVar

T = TypeVar("T")
T2_CO = TypeVar("T2_CO", covariant=True)

Composable = Callable[[T], T]


class SupportedJoin(StrEnum):
    """
    Enum representing supported SQL join types.
    """

    LEFT = auto()
    RIGHT = auto()
    INNER = auto()
    LEFT_ANTI = auto()


@dataclass
class JoinCondition:
    """
    Represents a join condition between two DataFrame columns.

    Attributes:
        left_col (str): The column name from the left DataFrame.
        right_col (Optional[str]): The column name from the right DataFrame. Defaults to `left_col`.
        sign (Literal): The comparison operator (e.g., '==', '>', '<=', etc.).
    """  # noqa

    left_col: str
    right_col: Optional[str] = None
    sign: (
        Literal["=="]
        | Literal[">"]
        | Literal["<"]
        | Literal["<="]
        | Literal[">="]  # noqa
    ) = "=="

    def __post_init__(self):
        """
        Ensures that right_col defaults to left_col if not provided.
        """
        if not self.right_col:
            self.right_col = self.left_col
