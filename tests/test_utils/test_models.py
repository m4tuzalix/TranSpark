from tests import InternalTestCase
from pyspark.sql import DataFrame
from unittest import mock
from transpark.models import ComposableDFModel, CachableDFModel, TransparkMixin
from transpark.utils.mappings import Transformation
from transpark.utils.decorators import transformation


class ComposableTestCase(InternalTestCase):
    def test__composablemodel__works_fine(self):
        df = mock.MagicMock(spec=DataFrame)

        # Chain withColumn
        df.withColumn.return_value = df

        composable = ComposableDFModel()
        dummy_func_1 = lambda df: df.withColumn("age", mock.ANY)  # noqa
        dummy_func_2 = lambda df: df.withColumn("first_letter", mock.ANY)  # noqa
        for num, _func in enumerate(
            [
                dummy_func_1,
                dummy_func_2,
            ],
            start=1,
        ):
            composable.add_transformation(
                Transformation(method=_func, order=num)
            )  # noqa
        assert composable.transformations == [
            Transformation(
                method=dummy_func_1, order=1, cache=False, cache_plan=False
            ),  # noqa
            Transformation(
                method=dummy_func_2, order=2, cache=False, cache_plan=False
            ),  # noqa
        ]

        result = composable.transform(starting_point=df)
        assert result == df

        result.withColumn.assert_has_calls(
            [
                mock.call("age", mock.ANY),
                mock.call("first_letter", mock.ANY),
            ],
            any_order=False,
        )


class CachableTestCase(InternalTestCase):
    def test__cachablemodel__multi_classes__distinct_storage_per_class(self):
        df = mock.MagicMock(spec=DataFrame)

        # Chain withColumn
        df.withColumn.return_value = df

        class DummyModel(CachableDFModel):

            def dummy_method_1(self, df):
                df.withColumn("dummy_column", "dummy_value")
                return df

        class DummyMode2(CachableDFModel):

            def dummy_method_2(self, df):
                df.withColumn("dummy_column2", "dummy_value2")
                return df

        dummy = DummyModel()
        dummy2 = DummyMode2()
        dummy._wrap_with_cache(dummy.dummy_method_1)
        dummy2._wrap_with_cache(dummy2.dummy_method_2)

        assert dummy.DFS_STORAGE is not dummy2.DFS_STORAGE

    def test__cachablemodel__fetch_once__cached_and_poped(self):
        df = mock.MagicMock(spec=DataFrame)
        df.is_cached = False
        # Chain withColumn
        df.withColumn.return_value = df

        class DummyModel(CachableDFModel):

            def dummy_method_1(self, df):
                df.withColumn("dummy_column", mock.ANY)
                return df

            def dummy_method_2(self):
                with self.fetch_once("dummy_method_1") as cached_method:
                    return cached_method

        dummy = DummyModel()
        cached_method = dummy._wrap_with_cache(dummy.dummy_method_1)
        assert cached_method(df) == df
        assert dummy.DFS_STORAGE == {"dummy_method_1": df}
        output = dummy.dummy_method_2()
        assert output == df
        assert dummy.DFS_STORAGE == {}


class TransparkMixinTestCase(InternalTestCase):
    def test__transpark_mixin__composable_and_cachable(self):
        df = mock.MagicMock(spec=DataFrame)
        df.is_cached = True
        # Chain withColumn
        df.withColumn.return_value = df

        class Dummy(TransparkMixin):
            cache = CachableDFModel
            transformator = ComposableDFModel

            @transformation(cache_plan=True)
            def dummy_method_1(self, df):
                return df.withColumn("age", mock.ANY)

            @transformation
            def dummy_method_2(self, df):
                return df.drop("age")

            @transformation
            def dummy_method_3(self, df):

                with self.cache.fetch_once("dummy_method_1") as cached_value:
                    return cached_value

            @transformation
            def dummy_method_4(self, df):
                return "dummy"

        mocked_cache = mock.MagicMock(spec=dict)
        mocked_cache.__contains__.side_effect = (
            lambda key: key == "dummy_method_1"
        )  # noqa
        mocked_cache.pop.side_effect = lambda key: df

        dummy = Dummy()
        object.__setattr__(dummy.cache, "DFS_STORAGE", mocked_cache)
        result = dummy.transformator.transform(df)

        assert result == "dummy"

        assert df.withColumn.call_args_list == [mock.call("age", mock.ANY)]
        assert df.drop.call_args_list == [mock.call("age")]
        assert mocked_cache.__contains__.call_args_list == [
            mock.call("dummy_method_1")
        ]  # noqa
        assert mocked_cache.pop.call_args_list == [mock.call("dummy_method_1")]
