from dataclasses import dataclass
from typing import (
    Optional,
    Generic,
    Any,
    overload,
)
from transpark.models.expected_output_model import ExpectedOutput
from pyspark.sql import DataFrame
from transpark.utils.mappings import T, Composable


@dataclass(frozen=True, slots=True)
class Transformation(Generic[T]):
    """
    A transformation step in a composable pipeline.

    Attributes:
        method (Composable[T]): The transformation function.
        order (int): Execution order in the transformation pipeline.
        cache (bool): Whether to cache the output of this transformation.
        cache_plan (bool): Whether to include this step in the cache planning process.
    """  # noqa

    method: Composable
    order: int
    cache: bool = False
    cache_plan: bool = False
    output_validation: Optional[ExpectedOutput] = None
    continue_on_failed_validation: bool = False

    def _process_df(self, method_result: DataFrame) -> DataFrame:
        if self.output_validation:
            try:
                self.output_validation.validate(method_result)
            except ValueError as e:
                if not self.continue_on_failed_validation:
                    raise ValueError(
                        f"{self.method.__self__.__class__}::{self.method.__name__}: {str(e)}"  # noqa
                    )
        if self.cache:
            method_result.cache()
        return method_result

    @overload
    def process(self, method_result: DataFrame) -> DataFrame: ...

    @overload
    def process(self, method_result: dict) -> dict: ...

    @overload
    def process(self, method_result: str) -> str: ...

    def process(self, method_result: Any) -> Any:
        if isinstance(method_result, DataFrame):
            return self._process_df(method_result=method_result)
        return method_result
