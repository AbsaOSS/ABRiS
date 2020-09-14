# Using ABRiS with Python and PySpark
Abris is a Scala library, but with a bit of effort it can be used in Python as well.

We Provide some examples, but most of the documentation is written for Scala, so if you need more check the scala examples and just convert the code to Python.

PySpark is using [Py4J](https://www.py4j.org/) as an interface between Scala and Python so you can check the documentation to get better idea how to transform the code, 
but mostly it should be clear form the following examples.

### Examples (!!! outdated)
Any help to update this to version 4.x.x is welcomed.
```Python
from pyspark import SparkContext
from pyspark.sql.column import Column, _to_java_column

def from_avro(col, topic, schema_registry_url):
    """
    avro deserialize

    :param col: column name "key" or "value"
    :param topic: kafka topic
    :param schema_registry_url: schema registry http address
    :return:
    """
    jvm_gateway = SparkContext._active_spark_context._gateway.jvm
    abris_avro = jvm_gateway.za.co.absa.abris.avro
    naming_strategy = getattr(
        getattr(abris_avro.read.confluent.SchemaManager, "SchemaStorageNamingStrategies$"),
        "MODULE$"
    ).TOPIC_NAME()

    schema_registry_config_dict = {
        "schema.registry.url": schema_registry_url,
        "schema.registry.topic": topic,
        "{col}.schema.id".format(col=col): "latest",
        "{col}.schema.naming.strategy".format(col=col): naming_strategy
    }

    conf_map = getattr(getattr(jvm_gateway.scala.collection.immutable.Map, "EmptyMap$"), "MODULE$")
    for k, v in schema_registry_config_dict.items():
        conf_map = getattr(conf_map, "$plus")(jvm_gateway.scala.Tuple2(k, v))

    return Column(abris_avro.functions.from_confluent_avro(_to_java_column(col), conf_map))


def to_avro(col, topic, schema_registry_url):
    """
    avro  serialize
    :param col: column name "key" or "value"
    :param topic: kafka topic
    :param schema_registry_url: schema registry http address
    :return:
    """
    jvm_gateway = SparkContext._active_spark_context._gateway.jvm
    abris_avro = jvm_gateway.za.co.absa.abris.avro
    naming_strategy = getattr(
        getattr(abris_avro.read.confluent.SchemaManager, "SchemaStorageNamingStrategies$"),
        "MODULE$"
    ).TOPIC_NAME()

    schema_registry_config_dict = {
        "schema.registry.url": schema_registry_url,
        "schema.registry.topic": topic,
        "{col}.schema.id".format(col=col): "latest", 
        "{col}.schema.naming.strategy".format(col=col): naming_strategy
    }

    conf_map = getattr(getattr(jvm_gateway.scala.collection.immutable.Map, "EmptyMap$"), "MODULE$")
    for k, v in schema_registry_config_dict.items():
        conf_map = getattr(conf_map, "$plus")(jvm_gateway.scala.Tuple2(k, v))

    return Column(abris_avro.functions.to_confluent_avro(_to_java_column(col), conf_map))
```
