from unittest import TestCase
from pyspark.sql import SparkSession
import os
import sys

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable


class InternalTestCase(TestCase):
    pass


class InternalE2ETestCase(TestCase):
    def setUp(self):
        self.spark: SparkSession = (
            SparkSession.builder.master("local[*]")
            .appName("transpark")
            .getOrCreate()  # noqa
        )
