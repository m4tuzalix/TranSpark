from typing import Protocol
from pyspark.sql.types import StructType
from pyspark.sql import DataFrame
from dataclasses import dataclass


class ExpectedOutput(Protocol):
    """A protocol for classes that can validate an output."""

    def validate(self, *args, **kwargs) -> bool:
        pass


@dataclass
class ExpectedOutputDF:
    """
    Validates that an output string matches the expected string.

    If the strings do not match, it raises a ValueError with a unified
    diff to clearly show the discrepancies.
    """

    expected_schema: StructType

    def validate(self, output: DataFrame) -> bool:
        """
        Compares the output string with the expected string.

        Args:
            output: The actual string result to validate.

        Returns:
            True if the strings match.

        Raises:
            ValueError: If the strings do not match, containing a diff.
            TypeError: If the output is not a string.
        """
        output_df_schema = output.schema

        expected_fields = {
            (f.name, f.dataType) for f in self.expected_schema.fields
        }  # noqa
        output_fields = {(f.name, f.dataType) for f in output_df_schema.fields}  # noqa

        if expected_fields != output_fields:
            missing = expected_fields - output_fields
            extra = output_fields - expected_fields

            msg = "; ".join(
                x
                for x, y in [
                    (f"Missing fields: {missing}", missing),
                    (f"Unexpected fields: {extra}", extra),
                ]
                if y
            )

            if msg:
                raise ValueError(msg)
        return True
