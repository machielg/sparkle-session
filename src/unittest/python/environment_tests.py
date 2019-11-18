from sparkle_test import SparkleTestCase

from sparkle_session.session import sparkle_session


class EnvironmentTestCase(SparkleTestCase):

    def setUp(self):
        self.spark = sparkle_session(self.spark)
        self.spark.conf.unset("spark.app.env")
        self.spark.conf.unset("spark.app.foo")
        self.spark.conf.unset("spark.app.dev.foo")
        self.spark.conf.unset("spark.app.acc.foo")
        self.spark.conf.unset("spark.app.prod.foo")
        if hasattr(self.spark.conf, "orig_get"):
            self.spark.conf.get = self.spark.conf.orig_get

    def test_plain_conf(self):
        self.spark.conf.set("foo", "bar")
        self.assertEqual("bar", self.spark.conf.get("foo"))

    def test_plain_spark_conf(self):
        self.spark.conf.set("spark.app.foo", "bar")
        self.assertEqual("bar", self.spark.conf.get("spark.app.foo"))

    def test_no_env_spark_conf(self):
        self.spark.conf.set("spark.app.dev.foo", "dev_bar")
        self.assertEqual("dev_bar", self.spark.conf.get("spark.app.dev.foo"))

    def test_env_conf(self):
        self.spark.conf.set("spark.app.dev.foo", "dev_bar")
        self.spark.conf.set("spark.app.acc.foo", "acc_bar")
        self.spark.conf.set("spark.app.env", "dev")
        self.assertEqual("dev_bar", self.spark.conf.get("spark.app.foo"))
        self.spark.conf.set("spark.app.env", "acc")
        self.assertEqual("acc_bar", self.spark.conf.get("spark.app.foo"))

    def test_default_conf(self):
        self.spark.conf.set("spark.app.env", "prod")
        self.spark.conf.set("spark.app.foo", "default_bar")
        self.assertEqual("default_bar", self.spark.conf.get("spark.app.foo"))
        self.spark.conf.set("spark.app.prod.foo", "prod_bar")
        self.assertEqual("prod_bar", self.spark.conf.get("spark.app.foo"))

    def test_replace_vals(self):
        self.spark.conf.set("spark.app.env", "prod")
        self.spark.conf.set("spark.app.foo", "/var/www/{{env}}/html")
        self.assertEqual("/var/www/prod/html", self.spark.conf.get("spark.app.foo"))

    def test_replace_vals_env_name(self):
        self.spark.conf.set("spark.app.env", "prod")
        self.spark.conf.set("spark.app.foo", "/var/www/{{env_name}}/html")
        self.assertEqual("/var/www/production/html", self.spark.conf.get("spark.app.foo"))

    def test_replace_vals_env_name_blank_env_name(self):
        self.spark.conf.set("spark.app.env", "prod")
        self.spark.conf.set("spark.app.prod.short_name", "")
        self.spark.conf.set("spark.app.prod.full_name", "")
        self.spark.conf.set("spark.app.foo", "/var/www/{{env_name}}/html")
        self.assertEqual("/var/www/html", self.spark.conf.get("spark.app.foo"))
        self.spark.conf.set("spark.app.foo", "{{env}}_db.table_name")
        self.assertEqual("db.table_name", self.spark.conf.get("spark.app.foo"))
