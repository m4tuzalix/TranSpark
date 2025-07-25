from enum import StrEnum, auto
from dataclasses import dataclass
from typing import (
    Literal,
    Optional,
    NamedTuple,
    Callable,
    TypeVar,
    Protocol,
    Generic,
)
from contextlib import AbstractContextManager

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


class Transformation(NamedTuple, Generic[T]):
    """
    A transformation step in a composable pipeline.

    Attributes:
        method (Composable[T]): The transformation function.
        order (int): Execution order in the transformation pipeline.
        cache (bool): Whether to cache the output of this transformation.
        cache_plan (bool): Whether to include this step in the cache planning process.
    """

    method: Composable[T]
    order: int
    cache: bool = False
    cache_plan: bool = False


@dataclass
class JoinCondition:
    """
    Represents a join condition between two DataFrame columns.

    Attributes:
        left_col (str): The column name from the left DataFrame.
        right_col (Optional[str]): The column name from the right DataFrame. Defaults to `left_col`.
        sign (Literal): The comparison operator (e.g., '==', '>', '<=', etc.).
    """

    left_col: str
    right_col: Optional[str] = None
    sign: (
        Literal["=="] | Literal[">"] | Literal["<"] | Literal["<="] | Literal[">="]
    ) = "=="

    def __post_init__(self):
        """
        Ensures that right_col defaults to left_col if not provided.
        """
        if not self.right_col:
            self.right_col = self.left_col


class TransformationClass(Protocol[T]):
    """
    Protocol defining the interface for composable transformation classes.
    """

    def add_transformation(self, transformation: Transformation[T]) -> None:
        """
        Add a transformation step to the pipeline.
        """
        ...

    def transform(self, starting_point: T) -> T:
        """
        Apply all transformations to the provided starting data.
        """
        ...


class CacheClass(Protocol[T2_CO]):
    """
    Protocol defining the interface for cache-handling classes.
    """

    def fetch_once(self, name: str) -> AbstractContextManager[T2_CO | None]:
        """
        Fetch a cached object by name. If it exists, it's returned once and removed from the cache.

        Args:
            name (str): Cache key name.

        Returns:
            Context manager yielding the cached object or None.
        """
        ...

    def clear_cache(self) -> None:
        """
        Clear all cached items and unpersist any Spark DataFrames.
        """
        ...
