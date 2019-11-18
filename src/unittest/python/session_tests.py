from sparkle_test import SparkleTestCase

from sparkle_session.session import session_sparkle


class SparkleSessionTestCase(SparkleTestCase):

    def test_empty_data_frame(self):
        session = session_sparkle(self.spark)
        df = session.emptyDataFrame()
        self.assertEqual(0, df.count())
        self.assertEqual(0, len(df.columns))

    def test_log(self):
        session = session_sparkle(self.spark)
        log = session.log()
        self.assertIsNotNone(log)
        log.error("some error trace")
