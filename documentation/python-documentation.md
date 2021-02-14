# Using ABRiS with Python and PySpark
Abris is a Scala library, but with a bit of effort it can be used in Python as well.

We Provide some examples, but most of the documentation is written for Scala, so if you need more check the scala examples and just convert the code to Python.

PySpark is using [Py4J](https://www.py4j.org/) as an interface between Scala and Python so you can check the documentation to get better idea how to transform the code, 
but mostly it should be clear form the following examples.

### Examples

```python
from pyspark import SparkContext
from pyspark.sql.column import Column, _to_java_column

def from_avro(col, config):
    """
    avro deserialize

    :param col (PySpark column / str): column name "key" or "value"
    :param config (za.co.absa.abris.config.FromAvroConfig): abris config, generated from abris_config helper function
    :return: PySpark Column
    """
    jvm_gateway = SparkContext._active_spark_context._gateway.jvm
    abris_avro = jvm_gateway.za.co.absa.abris.avro

    return Column(abris_avro.functions.from_avro(_to_java_column(col), config))

def to_avro(col, config):
    """
    avro serialize
    :param col (PySpark column / str): column name "key" or "value"
    :param config (za.co.absa.abris.config.FromAvroConfig): abris config, generated from abris_config helper function
    :return: PySpark Column
    """
    jvm_gateway = SparkContext._active_spark_context._gateway.jvm
    abris_avro = jvm_gateway.za.co.absa.abris.avro

    return Column(abris_avro.functions.to_avro(_to_java_column(col), config))

def abris_config(config_map, topic, is_key):
    """
    Create abris config with a schema url

    :param config_map (dict[str, str]): configuration map to pass to deserializer, ex: {'schema.registry.url': 'http://localhost:8081'}
    :param topic (str): kafka topic
    :param is_key (bool): boolean
    :return: za.co.absa.abris.config.FromAvroConfig
    """
    jvm_gateway = SparkContext._active_spark_context._gateway.jvm
    scala_map = jvm_gateway.PythonUtils.toScalaMap(config_map)

    return jvm_gateway.za.co.absa.abris.config \
        .AbrisConfig \
        .fromConfluentAvro() \
        .downloadReaderSchemaByLatestVersion() \
        .andTopicNameStrategy(topic, is_key) \
        .usingSchemaRegistry(scala_map)
```

Complete Example with loading from Kafka:

```python
df = spark.read.format("kafka").option("kafka.bootstrap.servers", "localhost:9092").option("subscribe", "topic_name").load()

abris_settings = abris_config({'schema.registry.url': 'http://localhost:8081'}, 'topic_name', False)
df2 = df.withColumn("parsed", from_avro("value", abris_settings))
df2.show()
```
