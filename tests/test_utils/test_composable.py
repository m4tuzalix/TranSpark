from tests import InternalTestCase
from pyspark.sql import DataFrame
from unittest import mock
from transpark.utils.composable import compose
from transpark.utils.transformation import Transformation


class ComposableTestCase(InternalTestCase):
    def test__compose__methods_chained__all_method_applied(self):
        df = mock.MagicMock(spec=DataFrame)

        # Chain withColumn
        df.withColumn.return_value = df

        composable = compose(
            Transformation(lambda df: df.withColumn("age", mock.ANY), 1),
            Transformation(
                lambda df: df.withColumn("first_letter", mock.ANY), 2
            ),  # noqa
        )

        composable(df)

        assert df.withColumn.call_args_list == [
            mock.call("age", mock.ANY),
            mock.call("first_letter", mock.ANY),
        ]
