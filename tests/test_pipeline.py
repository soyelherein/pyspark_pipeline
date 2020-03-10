import unittest
import json
from tests.PySparkTest import PySparkTest
from pyspark.sql import Row, DataFrame
from utils.schema import input_schema, op_schema
from jobs import pipeline
import pandas as pd


class MyTestCase(PySparkTest):
    """To do"""

    def setUp(self):
        with open('tests/fixtures/fixtures.json') as f:
            self.fixture = json.load(f)
            data = self.fixture.get("input_data")
            data_row = [Row(**x) for x in data]
            self.df = self.spark.createDataFrame(data_row, schema=input_schema)
            op_data = [Row(**x) for x in self.fixture.get("expected_op")]
            self.expected_df = self.spark.createDataFrame(op_data, schema=op_schema)
            # self.df.write.json(self.fixture.get("input_path"), mode="overwrite")

    def test_transform(self):
        """To do"""

        op_df = pipeline.transform(self.spark, self.df)
        op_df_sorted = op_df.toPandas().sort_values(by=['id', 'event_name', 'created_at'], axis=0).set_index('id')
        exp_df_sorted = self.expected_df.toPandas().sort_values(by=['id', 'event_name', 'created_at'], axis=0).set_index('id')

        self.assertIsInstance(op_df, DataFrame, "Not a DataFrame")
        self.assertEqual(op_df.schema, op_schema, "Schema mismatch")
        self.assertEqual(op_df.count(), 10, "Count Mismatch")
        pd.testing.assert_frame_equal(
            op_df_sorted,
            exp_df_sorted)


if __name__ == '__main__':
    unittest.main()
