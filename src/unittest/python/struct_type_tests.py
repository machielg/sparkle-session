from unittest import TestCase

from pyspark.sql.types import StructType, IntegerType, StructField, ArrayType, StringType

from sparkle_session.struct_type import sparkle_struct_type, SparkleStructType


class SparkleStructTypeTestCase(TestCase):

    def test_str_type_names(self):
        s = StructType(fields=[StructField("a", IntegerType()), StructField("b", IntegerType())])
        s = sparkle_struct_type(s)
        self.assertListEqual(['a', 'b'], s.colsOfType('int'))
        self.assertListEqual(['a', 'b'], s.colsOfType('integer'))

    def test_array_type(self):
        s = StructType(fields=[StructField("a", ArrayType(StringType()))])
        s = sparkle_struct_type(s)
        self.assertListEqual(['a'], s.colsOfType('array'))
        self.assertListEqual(['a'], s.colsOfType('array<string>'))
        self.assertListEqual(['a'], s.colsOfType(ArrayType))
        self.assertListEqual(['a'], s.colsOfType(ArrayType(StringType())))
        self.assertListEqual([], s.colsOfType('int'))
        self.assertListEqual([], s.colsOfType('array<int>'))
        self.assertListEqual([], s.colsOfType(ArrayType(IntegerType())))

    def test_data_type_names(self):
        s = SparkleStructType(fields=[StructField("a", IntegerType()), StructField("b", IntegerType())])
        self.assertListEqual(['a', 'b'], s.colsOfType(IntegerType()))
        self.assertListEqual(['a', 'b'], s.colsOfType(IntegerType))
