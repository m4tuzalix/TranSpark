from tests import InternalE2ETestCase
from transpark.utils import transformation
from transpark.models import TransparkMixin, CachableDFModel, ComposableDFModel
from pyspark.sql import DataFrame, functions as f, Row
import pytest
from transpark.pipelines import TransparkDFWorker
from transpark.models.expected_output_model import ExpectedOutputDF
from pyspark.sql.types import StructType, StringType, StructField


@pytest.mark.integration
class TestE2EModel(InternalE2ETestCase):
    def test__e2e_model__works_fine__df_worker(self):
        class Dummy(TransparkDFWorker):

            @transformation
            def transformation_1(self, df: DataFrame) -> DataFrame:
                return df.withColumn("dummy1", f.lit("dummy1"))

            @transformation
            def transformation_2(self, df: DataFrame) -> DataFrame:
                return df.withColumn("dummy2", f.lit("dummy2"))

            @transformation
            def transformation_3(self, df: DataFrame) -> DataFrame:
                return df.withColumn("dummy3", f.lit("dummy3"))

        input_df = self.spark.createDataFrame(
            [{"name": "Andy"}, {"name": "Cristine"}]
        )  # noqa

        dummy_cls = Dummy()
        result = dummy_cls.transformator.transform(input_df)

        assert result.collect() == [
            Row(name="Andy", dummy1="dummy1", dummy2="dummy2", dummy3="dummy3"),  # noqa
            Row(
                name="Cristine",
                dummy1="dummy1",
                dummy2="dummy2",
                dummy3="dummy3",  # noqa
            ),
        ]

    def test__e2e_model__works_fine(self):
        class Dummy(TransparkMixin):
            cache = CachableDFModel
            transformator = ComposableDFModel

            @transformation
            def transformation_1(self, df: DataFrame) -> DataFrame:
                return df.withColumn("dummy1", f.lit("dummy1"))

            @transformation
            def transformation_2(self, df: DataFrame) -> DataFrame:
                return df.withColumn("dummy2", f.lit("dummy2"))

            @transformation
            def transformation_3(self, df: DataFrame) -> DataFrame:
                return df.withColumn("dummy3", f.lit("dummy3"))

        input_df = self.spark.createDataFrame(
            [{"name": "Andy"}, {"name": "Cristine"}]
        )  # noqa

        dummy_cls = Dummy()
        result = dummy_cls.transformator.transform(input_df)

        assert result.collect() == [
            Row(name="Andy", dummy1="dummy1", dummy2="dummy2", dummy3="dummy3"),  # noqa
            Row(
                name="Cristine",
                dummy1="dummy1",
                dummy2="dummy2",
                dummy3="dummy3",  # noqa
            ),
        ]

    def test__e2e_model__different_order__works_fine(self):
        class Dummy(TransparkMixin):
            cache = CachableDFModel
            transformator = ComposableDFModel

            @transformation(order=2)
            def transformation_1(self, df: DataFrame) -> DataFrame:
                return df.withColumn("dummy1", f.lit("dummy1"))

            @transformation(order=1)
            def transformation_2(self, df: DataFrame) -> DataFrame:
                return df.withColumn("dummy2", f.lit("dummy2"))

            @transformation(order=3)
            def transformation_3(self, df: DataFrame) -> DataFrame:
                return df.withColumn("dummy3", f.lit("dummy3"))

        input_df = self.spark.createDataFrame(
            [{"name": "Andy"}, {"name": "Cristine"}]
        )  # noqa

        dummy_cls = Dummy()
        result = dummy_cls.transformator.transform(input_df)
        assert result.collect() == [
            Row(name="Andy", dummy2="dummy2", dummy1="dummy1", dummy3="dummy3"),  # noqa
            Row(
                name="Cristine",
                dummy2="dummy2",
                dummy1="dummy1",
                dummy3="dummy3",  # noqa
            ),
        ]

    def test__e2e_model__cache__no_context_manager__works_fine(self):
        class Dummy(TransparkMixin):
            cache = CachableDFModel
            transformator = ComposableDFModel

            @transformation(cache=True)
            def transformation_1(self, df: DataFrame) -> DataFrame:
                return df.withColumn("dummy1", f.lit("dummy1"))

            @transformation
            def transformation_2(self, df: DataFrame) -> DataFrame:
                return df.withColumn("dummy2", f.lit("dummy2"))

            @transformation
            def transformation_3(self, df: DataFrame) -> DataFrame:
                with self.cache.fetch_once("transformation_1") as df_cached:
                    # df is cached
                    assert df_cached.is_cached

                # method poped from storage
                assert "transformation_1" not in self.cache.DFS_STORAGE
                return df.withColumn("dummy3", f.lit("dummy3"))

        input_df = self.spark.createDataFrame(
            [{"name": "Andy"}, {"name": "Cristine"}]
        )  # noqa

        dummy = Dummy()
        result = dummy.transformator.transform(input_df)
        assert result.collect() == [
            Row(name="Andy", dummy1="dummy1", dummy2="dummy2", dummy3="dummy3"),  # noqa
            Row(
                name="Cristine",
                dummy1="dummy1",
                dummy2="dummy2",
                dummy3="dummy3",  # noqa
            ),
        ]

    def test__e2e_model__cache_plan__works_fine(self):
        class Dummy(TransparkMixin):
            cache = CachableDFModel
            transformator = ComposableDFModel

            @transformation(cache_plan=True)
            def transformation_1(self, df: DataFrame) -> DataFrame:
                return df.withColumn("dummy1", f.lit("dummy1"))

            @transformation
            def transformation_2(self, df: DataFrame) -> DataFrame:
                return df.withColumn("dummy2", f.lit("dummy2"))

            @transformation
            def transformation_3(self, df: DataFrame) -> DataFrame:
                with self.cache.fetch_once("transformation_1") as df_cached:
                    # df is not cached, just recalculated based on cached plan
                    assert not df_cached.is_cached

                # method poped from storage
                assert "transformation_1" not in self.cache.DFS_STORAGE
                return df.withColumn("dummy3", f.lit("dummy3"))

        input_df = self.spark.createDataFrame(
            [{"name": "Andy"}, {"name": "Cristine"}]
        )  # noqa

        dummy = Dummy()
        result = dummy.transformator.transform(input_df)
        assert result.collect() == [
            Row(name="Andy", dummy1="dummy1", dummy2="dummy2", dummy3="dummy3"),  # noqa
            Row(
                name="Cristine",
                dummy1="dummy1",
                dummy2="dummy2",
                dummy3="dummy3",  # noqa
            ),
        ]

    def test__e2e_model__cache__context_manager__cache_cleared__works_fine(
        self,
    ):  # noqa
        class Dummy(TransparkMixin):
            cache = CachableDFModel
            transformator = ComposableDFModel

            @transformation(cache=True)
            def transformation_1(self, df: DataFrame) -> DataFrame:
                return df.withColumn("dummy1", f.lit("dummy1"))

            @transformation
            def transformation_2(self, df: DataFrame) -> DataFrame:
                return df.withColumn("dummy2", f.lit("dummy2"))

            @transformation
            def transformation_3(self, df: DataFrame) -> DataFrame:
                return df.withColumn("dummy3", f.lit("dummy3"))

        input_df = self.spark.createDataFrame(
            [{"name": "Andy"}, {"name": "Cristine"}]
        )  # noqa

        with Dummy() as dummy:
            result = dummy.transformator.transform(input_df)
            assert "transformation_1" in dummy.cache.DFS_STORAGE
            assert isinstance(
                list(dummy.cache.DFS_STORAGE.values())[0], DataFrame
            )  # noqa

        assert not dummy.cache.DFS_STORAGE
        assert result.collect() == [
            Row(name="Andy", dummy1="dummy1", dummy2="dummy2", dummy3="dummy3"),  # noqa
            Row(
                name="Cristine",
                dummy1="dummy1",
                dummy2="dummy2",
                dummy3="dummy3",  # noqa
            ),
        ]

    def test__e2e_model__cache__context_manager__cache_cleared__model_validation__works_fine(
        self,
    ):  # noqa
        class Dummy(TransparkMixin):
            cache = CachableDFModel
            transformator = ComposableDFModel

            @transformation(
                cache=True,
                expected_output=ExpectedOutputDF(
                    expected_schema=StructType(
                        [
                            StructField("name", StringType(), True),
                            StructField("dummy1", StringType(), True),
                        ]
                    )
                ),
            )
            def transformation_1(self, df: DataFrame) -> DataFrame:
                return df.withColumn("dummy1", f.lit("dummy1"))

            @transformation(
                expected_output=ExpectedOutputDF(
                    expected_schema=StructType(
                        [
                            StructField("name", StringType(), True),
                            StructField("dummy1", StringType(), True),
                            StructField("dummy2", StringType(), True),
                        ]
                    )
                )
            )
            def transformation_2(self, df: DataFrame) -> DataFrame:
                return df.withColumn("dummy2", f.lit("dummy2"))

            @transformation(
                expected_output=ExpectedOutputDF(
                    expected_schema=StructType(
                        [
                            StructField("name", StringType(), True),
                            StructField("dummy1", StringType(), True),
                            StructField("dummy2", StringType(), True),
                            StructField("dummy3", StringType(), True),
                        ]
                    )
                )
            )
            def transformation_3(self, df: DataFrame) -> DataFrame:
                return df.withColumn("dummy3", f.lit("dummy3"))

        input_df = self.spark.createDataFrame(
            [{"name": "Andy"}, {"name": "Cristine"}]
        )  # noqa

        with Dummy() as dummy:
            result = dummy.transformator.transform(input_df)
            assert "transformation_1" in dummy.cache.DFS_STORAGE
            assert isinstance(
                list(dummy.cache.DFS_STORAGE.values())[0], DataFrame
            )  # noqa

        assert not dummy.cache.DFS_STORAGE
        assert result.collect() == [
            Row(name="Andy", dummy1="dummy1", dummy2="dummy2", dummy3="dummy3"),  # noqa
            Row(
                name="Cristine",
                dummy1="dummy1",
                dummy2="dummy2",
                dummy3="dummy3",  # noqa
            ),
        ]

    def test__e2e_model__cache__context_manager__cache_cleared__model_validation_failed__works_fine(
        self,
    ):  # noqa
        class Dummy(TransparkMixin):
            cache = CachableDFModel
            transformator = ComposableDFModel

            @transformation(
                cache=True,
                expected_output=ExpectedOutputDF(
                    expected_schema=StructType(
                        [
                            StructField("name", StringType(), True),
                            StructField("dummy1", StringType(), True),
                        ]
                    )
                ),
            )
            def transformation_1(self, df: DataFrame) -> DataFrame:
                return df.withColumn("dummy1", f.lit("dummy1"))

            @transformation(
                expected_output=ExpectedOutputDF(
                    expected_schema=StructType(
                        [
                            StructField("name", StringType(), True),
                            StructField("dummy1", StringType(), True),
                            StructField("dummy2", StringType(), True),
                        ]
                    )
                )
            )
            def transformation_2(self, df: DataFrame) -> DataFrame:
                return df.withColumn("dummy2", f.lit("dummy2"))

            @transformation(
                expected_output=ExpectedOutputDF(
                    expected_schema=StructType(
                        [
                            StructField("name", StringType(), True),
                            StructField("dummy1", StringType(), True),
                            StructField("dummy2", StringType(), True),
                            StructField("dummy4", StringType(), True),
                        ]
                    )
                )
            )
            def transformation_3(self, df: DataFrame) -> DataFrame:
                return df.withColumn("dummy3", f.lit("dummy3"))

        input_df = self.spark.createDataFrame(
            [{"name": "Andy"}, {"name": "Cristine"}]
        )  # noqa

        with pytest.raises(ValueError) as e, Dummy() as dummy:
            dummy.transformator.transform(input_df)

        assert (
            "Missing fields: {('dummy4', StringType())}; Unexpected fields: {('dummy3', StringType())}"
            in str(e.value)
        )
        assert not dummy.cache.DFS_STORAGE
