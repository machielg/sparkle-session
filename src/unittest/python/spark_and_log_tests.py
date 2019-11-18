from sparkle_test import SparkleTestCase

from sparkle_session.spark_and_log import SparkAndLog


class MyClass(SparkAndLog):
    log_level = "DEBUG"


class SparkAndLogTestCase(SparkleTestCase):

    def test_create_class(self):
        c = MyClass()
        self.assertIsNotNone(c.log)

        c.log.info("Foo!")
        c.log.dbg("Can you count from {} to {}", lambda: (1, 2))
