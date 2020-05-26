# ABRiS - vanilla Avro documentation

- [Avro to Spark](#Avro-to-Spark)
- [Spark to Avro](#spark-to-avro)
## Avro to Spark

### Providing Avro schema    
```scala
import za.co.absa.abris.avro.functions.from_avro

def readAvro(dataFrame: DataFrame, schemaString: String): DataFrame = {

  dataFrame.select(from_avro(col("value"), schemaString) as 'data).select("data.*")
}
```
In this example the Avro binary data are in ```dataFrame``` inside column the **value**. The Avro schema is provided as a string ```schemaString```.

After the Avro data are converted to Spark SQL representation they are stored in column the **data**. This column is immediately flattened in the next select so the result will be a ```DataFrame``` containing only the deserialized avro data.  

### Using Schema Registry
If you want to use Schema Registry you need to provide a configuration:

```scala
val schemaRegistryConfig = Map(
  SchemaManager.PARAM_SCHEMA_REGISTRY_URL          -> "url_to_schema_registry",
  SchemaManager.PARAM_SCHEMA_REGISTRY_TOPIC        -> "topic_name",
  SchemaManager.PARAM_VALUE_SCHEMA_NAMING_STRATEGY -> SchemaManager.SchemaStorageNamingStrategies.{TOPIC_NAME, RECORD_NAME, TOPIC_RECORD_NAME}, // choose a subject name strategy
  SchemaManager.PARAM_VALUE_SCHEMA_ID              -> "current_schema_id" // set to "latest" if you want the latest schema version to used  
  SchemaManager.PARAM_VALUE_SCHEMA_VERSION         -> "current_schema_version" // set to "latest" if you want the latest schema version to used
)
```
Depending on the selected naming strategy you may also need to provide ```SchemaManager.PARAM_SCHEMA_NAME_FOR_RECORD_STRATEGY``` and ```SchemaManager.PARAM_SCHEMA_NAMESPACE_FOR_RECORD_STRATEGY```
 
```scala
import za.co.absa.abris.avro.functions.from_avro

def readAvro(dataFrame: DataFrame, schemaRegistryConfig: Map[String, String]): DataFrame = {

  dataFrame.select(from_avro(col("value"), schemaRegistryConfig) as 'data).select("data.*")
}
```
This example is similar to the previous one except for the fact that this time configurations to access Schema Registry are provided instead of the schema itself. The schema must already be available on Schema Registry.

## Spark to Avro


### Writing Avro records 
```scala
import za.co.absa.abris.avro.functions.to_avro

def writeAvro(dataFrame: DataFrame): DataFrame = {

  val allColumns = struct(dataFrame.columns.head, dataFrame.columns.tail: _*)
  dataFrame.select(to_avro(allColumns) as 'value)
}
```
This is the simplest possible usage of the ```to_avro``` expression. We just provide the column that we want to serialize and the library will generate the schema automatically from Spark data types. 

If you want to serialize more than one column, you have to put them in a Spark *struct* first, as shown in the example.

### Providing Avro schema 
```scala
import za.co.absa.abris.avro.functions.to_avro

def writeAvro(dataFrame: DataFrame, schemaString: String): DataFrame = {

  val allColumns = struct(dataFrame.columns.head, dataFrame.columns.tail: _*)
  dataFrame.select(to_avro(allColumns, schemaString) as 'value)
}
```
If you provide the Avro schema as a second argument, ABRiS will use it to convert Spark data into Avro. Please make sure that the data types in Spark DataFrame and in schema are compatible.

### Using schema registry
First we need to provide the Schema Registry configuration:
```scala
val schemaRegistryConfig = Map(
  SchemaManager.PARAM_SCHEMA_REGISTRY_URL                        -> "url_to_schema_registry",
  SchemaManager.PARAM_SCHEMA_REGISTRY_TOPIC                      -> "topic_name",
  SchemaManager.PARAM_VALUE_SCHEMA_NAMING_STRATEGY               -> SchemaManager.SchemaStorageNamingStrategies.TOPIC_RECORD_NAME,
  SchemaManager.PARAM_VALUE_SCHEMA_NAME_FOR_RECORD_STRATEGY      -> "schema_name",
  SchemaManager.PARAM_VALUE_SCHEMA_NAMESPACE_FOR_RECORD_STRATEGY -> "schema_namespace"
)
```
In this example the ```TOPIC_RECORD_NAME``` naming strategy is used, therefore we need to provide topic, name and namespace.

```scala
import za.co.absa.abris.avro.functions.to_avro

def writeAvro(dataFrame: DataFrame, schemaRegistryConfig: Map[String, String]): DataFrame = {

  val allColumns = struct(dataFrame.columns.head, dataFrame.columns.tail: _*)  
  dataFrame.select(to_avro(allColumns, schemaRegistryConfig) as 'value)
}
```
Since we didn't provide a schema it will be generated automatically and then stored into Schema Registry.

You can also set `SchemaManager.PARAM_VALUE_SCHEMA_ID` or `SchemaManager.PARAM_VALUE_SCHEMA_VERSION` and Abris will use it to download the schema and convert the data according to the specified schema. 
In that case no new schema will be registered.


### Using schema registry and providing a schema
You can also provide the schema string directly:
```scala
import za.co.absa.abris.avro.functions.to_avro

def writeAvro(dataFrame: DataFrame, schemaString: String, schemaRegistryConfig: Map[String, String]): DataFrame = {

  val allColumns = struct(dataFrame.columns.head, dataFrame.columns.tail: _*)  
  dataFrame.select(to_avro(allColumns, schemaString, schemaRegistryConfig) as 'value)
}
```
