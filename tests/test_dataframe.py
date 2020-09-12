import pyspark.sql.functions as f
from pyspark.sql.types import LongType
from sparkle_test import SparkleTestCase

from sparkle_session import SparkleStructType
from sparkle_session.dataframe import sparkle_df, SparkleDataFrame


class SparkleDataFrameTestCase(SparkleTestCase):

    def setUp(self):
        df = self.spark.createDataFrame([(1, 2), (3, 4)], ["a", "b"])
        self.sdf = sparkle_df(df)

    def test_to_ddl(self):
        self.assertEqual("`a` BIGINT,`b` BIGINT", self.sdf.toDDL())
        self.assertEqual("`a` BIGINT", self.sdf.toDDL("b"))
        self.assertEqual("", self.sdf.toDDL("a", "b"))

    def test_drop(self):
        df = self.spark.createDataFrame([(1, "y"), (3, "z")], ["a", "b"])
        sdf = sparkle_df(df)
        self.assertNotIn("a", sdf.dropOfType('bigint').columns)
        self.assertIn("b", sdf.dropOfType('bigint').columns)
        self.assertNotIn("b", sdf.dropOfType('string').columns)

    def test_all_any(self):
        df = self.spark.createDataFrame([(1, 2), (3, 4)], ["a", "b"])
        sdf = sparkle_df(df)
        self.assertIsNotNone(sdf)
        self.assertEqual(1, sdf.filter("a == 1").count())
        self.assertFalse(sdf.all('a == 1'))
        self.assertTrue(sdf.all('a == 1 OR a == 3'))
        self.assertTrue(sdf.any('a == 1'))

    def test_cols_same(self):
        df1 = self.spark.createDataFrame([(1, 2)], ["a", "b"])
        df2 = self.spark.createDataFrame([(1, 2)], ["b", "c"])
        sdf1 = sparkle_df(df1)
        sdf2 = sparkle_df(df2)
        self.assertEqual(len(sdf1.columns), len(sdf2.columns))
        self.assertTrue(sdf1.hasSameColumns(sdf1))
        self.assertTrue(sdf2.hasSameColumns(sdf2))
        self.assertFalse(sdf1.hasSameColumns(sdf2))
        self.assertFalse(sdf2.hasSameColumns(sdf1))

    def test_require_column(self):
        df1 = self.spark.createDataFrame([(1, 2)], ["a", "b"])
        sdf1 = sparkle_df(df1)
        sdf1.requireColumn("a")
        sdf1.requireColumn("b")
        sdf1.requireColumn(("a", LongType))
        # noinspection PyTypeChecker
        sdf1.requireColumn(("b", LongType()))

    def test_max_value(self):
        df1 = self.spark.createDataFrame([(1,), (2,), (0,)], ["a"])
        sdf1 = sparkle_df(df1)
        self.assertEqual(2, sdf1.maxValue("a"))

    def test_stay_sparkle(self):
        df1 = self.spark.createDataFrame([(1,)], ["a"])
        sdf1 = sparkle_df(df1)
        self.assertIsInstance(sdf1, SparkleDataFrame)
        self.assertIsInstance(sdf1.select("a"), SparkleDataFrame)
        self.assertIsInstance(sdf1.drop("a"), SparkleDataFrame)
        self.assertIsInstance(sdf1.sort('a'), SparkleDataFrame)
        self.assertIsInstance(sdf1.schema, SparkleStructType)

        self.assertIsInstance(sdf1.groupBy('a').agg(f.max('a'), f.max('a')), SparkleDataFrame)

