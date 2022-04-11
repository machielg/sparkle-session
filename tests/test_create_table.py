from tempfile import mkdtemp

from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, DateType, StructType
from sparkle_test import SparkleTestCase

from sparkle_session.dataframe import sparkle_df


class CreateTableTestCase(SparkleTestCase):

    def setUp(self):
        s = SparkSession.builder.getOrCreate()
        s.sql("DROP TABLE IF EXISTS foo")
        df = s.createDataFrame([(1, 2)], ["a", "b"])
        self.df = sparkle_df(df)
        self.spark.conf.unset("spark.app.env")

    def test_create_table_with_partitions(self):
        partition_cols = StructType([StructField("part_date", DateType())])
        r = self.df.createTable('foo', partition_cols=partition_cols)
        self.assertIsNotNone(r)
        f = self.spark.table("foo")
        self.assertEqual(r.collect(), f.collect())
        self.assertIsNotNone(f)
        self.assertEqual('struct<a:bigint,b:bigint,part_date:date>', f.schema.simpleString())

    def test_create_external_table(self):
        table_location = mkdtemp()
        self.spark.conf.set("spark.app.env", "dev")
        r = self.df.createTable('foo', location=table_location)
        self.assertIsNotNone(r)
        create_stmt = self.spark.sql("SHOW CREATE TABLE foo").first()['createtab_stmt']
        self.assertIn("LOCATION", create_stmt)
        self.assertIn(table_location, create_stmt)
        self.assertIn('parquet', create_stmt.lower())
