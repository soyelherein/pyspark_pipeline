"""
PySparkTest.py
~~~~~~~~~~~~~~~~~~~
This module defines the unittest setup method further used for
test cases defined in test_pipeline.py.
"""
import unittest
from utils.utilities import start_spark

__author__ = 'Soyel Alam'


class PySparkTest(unittest.TestCase):
    """
    Class responsible for starting and stopping spark session
    for unit testing
    """

    @classmethod
    def setUpClass(cls):
        """Start Spark and get a Py4j logger for unit testing"""
        cls.spark, cls.logger = start_spark('unittest', 'local')

    @classmethod
    def tearDown(cls):
        """Stop the Spark session created for unit testing"""
        cls.spark.stop()
