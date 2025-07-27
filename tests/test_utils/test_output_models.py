from tests import InternalTestCase
from pyspark.sql import DataFrame
from unittest import mock
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
)
import pytest
from transpark.models.expected_output_model import ExpectedOutputDF


class OutputModelsTestCase(InternalTestCase):
    def test__baseoutputmodel_validation__passed_works_fine(self):

        schema = StructType(
            [
                StructField("name", StringType(), True),
                StructField("age", IntegerType(), True),
                StructField("salary", DoubleType(), True),
            ]
        )
        mock_df = mock.MagicMock(spec=DataFrame)
        mock_df.schema.fields = schema.fields

        # Should not raise
        assert ExpectedOutputDF(
            expected_schema=StructType(
                [
                    StructField("name", StringType(), True),
                    StructField("age", IntegerType(), True),
                    StructField("salary", DoubleType(), True),
                ]
            )
        ).validate(mock_df)

    def test__baseoutputmodel_validation__failed__missing_field__works_fine(self):

        schema = StructType(
            [
                StructField("name", StringType(), True),
                StructField("age", IntegerType(), True),
            ]
        )
        mock_df = mock.MagicMock(spec=DataFrame)
        mock_df.schema.fields = schema.fields

        with pytest.raises(ValueError) as e:
            ExpectedOutputDF(
                expected_schema=StructType(
                    [
                        StructField("name", StringType(), True),
                        StructField("age", IntegerType(), True),
                        StructField("salary", DoubleType(), True),
                    ]
                )
            ).validate(mock_df)

        assert "Missing fields: {('salary', DoubleType())}" in str(e.value)

    def test__baseoutputmodel_validation__failed__field_type_mismatch__works_fine(self):

        schema = StructType(
            [
                StructField("name", StringType(), True),
                StructField("age", IntegerType(), True),
                StructField("salary", IntegerType(), True),
            ]
        )
        mock_df = mock.MagicMock(spec=DataFrame)
        mock_df.schema.fields = schema.fields

        with pytest.raises(ValueError) as e:
            ExpectedOutputDF(
                expected_schema=StructType(
                    [
                        StructField("name", StringType(), True),
                        StructField("age", IntegerType(), True),
                        StructField("salary", DoubleType(), True),
                    ]
                )
            ).validate(mock_df)

        assert (
            "Missing fields: {('salary', DoubleType())}; Unexpected fields: {('salary', IntegerType())}"
            in str(e.value)
        )
