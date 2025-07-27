from typing import Protocol, Optional
from pyspark.sql.types import StructType
from pyspark.sql import DataFrame
from dataclasses import dataclass


class ExpectedOutput(Protocol):
    def validate(self, *args, **kwargs) -> bool:
        pass


@dataclass
class ExpectedOutputDF:
    expected_schema: StructType

    def validate(self, output: DataFrame) -> bool:
        output_df_schema = output.schema

        expected_fields = {(f.name, f.dataType) for f in self.expected_schema.fields}
        output_fields = {(f.name, f.dataType) for f in output_df_schema.fields}

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
