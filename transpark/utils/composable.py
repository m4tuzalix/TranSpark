from functools import reduce
from pyspark.sql import DataFrame
from transpark.utils.transformation import Transformation, Composable, T
from typing import overload


@overload
def compose(*functions: Transformation[DataFrame]) -> Composable[DataFrame]: ...  # noqa


@overload
def compose(*functions: Transformation[dict]) -> Composable[dict]: ...


@overload
def compose(*functions: Transformation[str]) -> Composable[str]: ...


def compose(*functions: Transformation[T]) -> Composable[T]:
    """
    Compose multiple transformations into a single callable pipeline.

    Applies each transformation in sequence to the given input data. If the
    result of a transformation is a cached PySpark DataFrame, it will be cached.

    Args:
        *functions (Transformation[T]): A variable number of transformation
            steps to apply.

    Returns:
        Composable[T]: A single callable object that applies all transformations
        in sequence to input of type `T`.
    """  # noqa

    def apply(data: T, function: Transformation[T]) -> T:
        result: T = function.method(data)
        return function.process(result)

    return lambda data: reduce(apply, functions, data)
