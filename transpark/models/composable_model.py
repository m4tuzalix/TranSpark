from transpark.utils.composable import compose
from transpark.utils.mappings import Transformation
from pyspark.sql import DataFrame


class ComposableDFModel:
    """
    A model for composing and executing a sequence of DataFrame transformations.

    Uses `Transformation` objects to build a deterministic and reusable pipeline.
    """

    __slots__ = ("_transformations",)

    _transformations: list[Transformation[DataFrame]]

    def __init__(self):
        """
        Initialize the transformation pipeline with an empty list.
        """
        self._transformations = list()

    @property
    def transformations(self) -> list[Transformation[DataFrame]]:
        """
        Get the list of registered transformations.

        Returns:
            list[Transformation[DataFrame]]: The ordered list of transformations.
        """
        return self._transformations

    def add_transformation(self, transformation: Transformation[DataFrame]):
        """
        Add a transformation to the pipeline if it's not already present.

        Args:
            transformation (Transformation[DataFrame]): A transformation object to add.
        """
        if (
            isinstance(transformation, Transformation)
            and transformation not in self._transformations
        ):
            self._transformations.append(transformation)

    def transform(self, starting_point: DataFrame) -> DataFrame:
        """
        Apply all registered transformations to the given DataFrame.

        The transformations are sorted by their defined order (or fallback to insertion order)
        and composed into a single callable pipeline.

        Args:
            starting_point (DataFrame): The input DataFrame to transform.

        Returns:
            DataFrame: The resulting DataFrame after all transformations have been applied.
        """
        _transformations = [
            obj
            for _, obj in sorted(
                enumerate(self.transformations, start=1),
                key=lambda obj: (obj[1].order or obj[0]),
            )
        ]
        return compose(*_transformations)(starting_point)
