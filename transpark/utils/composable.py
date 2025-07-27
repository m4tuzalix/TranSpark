from functools import reduce
from pyspark.sql import DataFrame
from transpark.utils.mappings import Transformation, Composable, T
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
    """

    def apply(data: T, function: Transformation[T]) -> T:
        result: T = function.method(data)
        if isinstance(result, DataFrame):
            if validation_model := function.output_validation:
                try:
                    validation_model.validate(result)
                except ValueError as e:
                    if not function.continue_on_failed_validation:
                        raise ValueError(
                            f"{function.method.__self__.__class__}::{function.method.__name__}: {str(e)}"
                        )
            if function.cache:
                result.cache()
        return result

    return lambda data: reduce(apply, functions, data)
