from transpark.models import TransparkMixin, CachableDFModel, ComposableDFModel


class TransparkDFWorker(TransparkMixin):
    """
    Concrete pipeline class that combines composable transformation logic
    with DataFrame-level caching.

    This class uses:
        - CachableDFModel: for managing temporary DataFrame caching.
        - ComposableDFModel: for ordered transformation composition.

    Extend this class and define methods decorated with @transformation
    to create reusable transformation pipelines.
    """

    cache = CachableDFModel
    transformator = ComposableDFModel
