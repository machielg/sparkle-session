## Sparkle Session

Reduce code and repetition using the Sparkle Dataframe. 

```python
from sparkle_session import current_sparkle_session, sparkle_session


# create your spark session as you normally would
spark = SparkSession.builder.getOrCreate()

# put some Sparkle into your session
spark = sparkle_session(spark)
# OR
spark = current_sparkle_session()
```

### Added Spark Session functions ###

Extra methods:
- emptyDataFrame()
- log()

```python
spark.emptyDataFrame()
df.show()
# ++
# ||
# ++

log = spark.log("mylog")
log.info("some info message")
# 19/11/18 16:01:28 INFO mylog: some info message

log = spark.log("mylog", "error")
log.info("some info message")
# no output, only errors in logs
```


