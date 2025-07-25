from typing import Callable, Optional


def transformation(
    _func: Optional[Callable] = None,
    *,
    order: int = 0,
    cache: bool = False,
    cache_plan: bool = False,
):
    """
    A decorator for marking methods as transformation steps in a pipeline.

    This decorator supports both `@transformation` and `@transformation(...)` usage.
    It attaches metadata to the method to be later picked up by TransparkMeta.

    Args:
        _func (Callable, optional): The function being decorated (when used as @transformation).
        order (int): The order in which the transformation should be applied.
        cache (bool): Whether to cache the result of this transformation.
        cache_plan (bool): Whether to register this transformation in the caching plan.

    Returns:
        Callable: The decorated function with transformation metadata attached.
    """

    def decorator(func: Callable) -> Callable:
        setattr(func, "_is_transformation", True)
        setattr(func, "_order", order)
        setattr(func, "_cache", cache)
        setattr(func, "_cache_plan", cache_plan)
        return func

    if _func is None:
        # Used as @transformation(...)
        return decorator
    else:
        # Used as @transformation
        return decorator(_func)
