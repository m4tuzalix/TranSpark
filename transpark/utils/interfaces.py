from typing import (
    Protocol,
)
from contextlib import AbstractContextManager
from transpark.utils.mappings import T, T2_CO, Composable
from transpark.utils.transformation import Transformation


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
    """  # noqa

    def fetch_once(self, name: str) -> AbstractContextManager[T2_CO | None]:
        """
        Fetch a cached object by name. If it exists, it's returned once and removed from the cache.

        Args:
            name (str): Cache key name.

        Returns:
            Context manager yielding the cached object or None.
        """  # noqa
        ...

    def clear_cache(self) -> None:
        """
        Clear all cached items and unpersist any Spark DataFrames.
        """
        ...

    def _wrap_with_cache(self) -> Composable:
        """Start caching logic."""
        ...
