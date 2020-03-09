import unittest
from utils.utilities import start_spark


class PySparkTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.spark = start_spark('unittest', 'local')

    @classmethod
    def tearDown(cls) -> None:
        cls.spark.stop()
