from transpark.utils.mappings import Transformation, CacheClass, TransformationClass
from typing import Self, ClassVar


class TransparkMeta(type):
    """
    Metaclass that auto-wires caching and transformation logic into classes using TransparkMixin.

    Responsibilities:
    - Instantiates `cache` and `transformator` components.
    - Scans for methods decorated with `@transformation`.
    - Automatically wraps cacheable methods.
    - Registers transformations in execution order.
    """

    def __call__(cls, *args, **kwargs):
        """
        Create an instance and dynamically attach cache and transformation logic.

        Args:
            *args: Positional arguments passed to the class constructor.
            **kwargs: Keyword arguments passed to the class constructor.

        Returns:
            An initialized instance with transformation metadata registered.
        """
        instance = super().__call__(*args, **kwargs)
        setattr(instance, "cache", cls.cache())
        setattr(instance, "transformator", cls.transformator())

        for attr_name in dir(instance):
            method = getattr(instance, attr_name)
            if callable(method) and hasattr(method, "_is_transformation"):
                _order = getattr(method, "_order", None)
                _cache = getattr(method, "_cache", False)
                _cache_plan = getattr(method, "_cache_plan", False)

                if any([_cache, _cache_plan]):
                    method = instance.cache._wrap_with_cache(method)
                    setattr(instance, attr_name, method)  # rebind the wrapped method

                instance.transformator.add_transformation(
                    Transformation(
                        method=method,
                        order=_order,
                        cache=_cache,
                        cache_plan=_cache_plan,
                    )
                )
        return instance


class TransparkMixin(metaclass=TransparkMeta):
    """
    Mixin to be extended by user-defined transformation pipelines.

    This class:
    - Declares `cache` and `transformator` class attributes, to be set as types.
    - Enables context-managed usage to auto-clear cache on exit.
    """

    cache: ClassVar[type[CacheClass]]
    transformator: ClassVar[type[TransformationClass]]

    def __enter__(self, *args, **kwargs) -> Self:
        """
        Enter the context manager.

        Returns:
            Self: The current pipeline instance.
        """
        return self

    def __exit__(self, *args, **kwargs):
        """
        Exit the context manager, automatically clearing any cached DataFrames.
        """
        self.cache.clear_cache()
