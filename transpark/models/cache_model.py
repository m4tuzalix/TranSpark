from typing import Iterator
from pyspark.sql import DataFrame
from functools import wraps
from contextlib import contextmanager
from transpark.utils.mappings import Composable


class CachableDFModel:
    """
    A cache manager for intermediate PySpark DataFrames.

    Stores DataFrames by method name, allows single-use access via a context manager,
    and supports unpersisting cached data to manage memory.
    """  # noqa

    __slots__ = ("DFS_STORAGE",)

    DFS_STORAGE: dict[str, DataFrame]

    def __init__(self):
        """
        Initialize an empty cache storage for DataFrames.
        """
        self.DFS_STORAGE = dict()

    def __avaiable(self, name: str) -> bool:
        """
        Check if a DataFrame with the given name is available in the cache.

        Args:
            name (str): Name of the transformation method (cache key).

        Returns:
            bool: True if the DataFrame is cached, False otherwise.
        """
        return name in self.DFS_STORAGE

    @contextmanager
    def fetch_once(self, name: str) -> Iterator[DataFrame | None]:
        """
        Yield a cached DataFrame by name, removing it from the cache after use.

        If the DataFrame is cached and marked as `is_cached`, it will be unpersisted
        after the context block finishes.

        Args:
            name (str): The cache key (typically the method name).

        Yields:
            Optional[DataFrame]: The cached DataFrame, or None if not available.
        """  # noqa
        if self.__avaiable(name=name):
            df = self.DFS_STORAGE.pop(name)
            try:
                yield df
            finally:
                if df.is_cached:
                    df.unpersist()
        else:
            yield None

    def clear_cache(self) -> None:
        """
        Unpersist and clear all cached DataFrames.
        """
        if self.DFS_STORAGE:
            for _, df in self.DFS_STORAGE.items():
                if df.is_cached:
                    df.unpersist()
            self.DFS_STORAGE.clear()

    def _wrap_with_cache(self, func) -> Composable[DataFrame]:
        """
        Wrap a method to enable automatic caching of its DataFrame result.

        Args:
            func (Callable): A method that returns a DataFrame.

        Returns:
            Callable: A wrapped version of the method that stores its result in the cache.
        """  # noqa

        @wraps(func)
        def cache_wrapper(df: DataFrame) -> DataFrame:
            cache_key = func.__name__
            result_df = func(df)
            self.DFS_STORAGE[cache_key] = result_df
            return result_df

        return cache_wrapper
