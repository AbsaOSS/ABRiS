/*
 * Copyright 2018 ABSA Group Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package za.co.absa.abris.examples

import java.io.FileInputStream
import java.util.Properties

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import za.co.absa.abris.avro.format.SparkAvroConversions
import za.co.absa.abris.avro.parsing.utils.AvroSchemaUtils
import za.co.absa.abris.avro.schemas.policy.SchemaRetentionPolicies.{RETAIN_SELECTED_COLUMN_ONLY, SchemaRetentionPolicy}
import za.co.absa.abris.examples.data.generation.ComplexRecordsGenerator

/**
  * This example shows how ABRiS can be used to read/write Avro records from/to Parquet files.
  */
object AvroParquet {

  private val PARQUET_PATH = "testParquetDestination"
  private val AVRO_SCHEMA = "src\\test\\resources\\example_schema.avsc"
  
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("AvroParquetTest")
      .master("local[2]")
      .getOrCreate()
      
    spark.sparkContext.setLogLevel("info")

    writeAvroToParquet(PARQUET_PATH, 10, spark)

    // reads from the Parquet file and uses the data schema as the Dataframe schema
    val parquetDF = readParquetAsAvro(PARQUET_PATH, RETAIN_SELECTED_COLUMN_ONLY, spark)
    
    parquetDF.select("*").show()
  }

  /**
    * Produces ''numRecords'' random entries and stored them into ''destination'' as Parquet.
    */
  private def writeAvroToParquet(destination: String, numRecords: Int, spark: SparkSession) = {
    
    import spark.implicits._
    import za.co.absa.abris.avro.AvroSerDe._
    
    implicit val encoder = getEncoder()
        
    val rows = getRows(10)
    val dataframe = spark.sparkContext.parallelize(rows, 8).toDF()      

    dataframe.show()

    dataframe
      .toAvro(AVRO_SCHEMA)
      .write
      .mode(SaveMode.Overwrite)
      .parquet(destination)      
  }

  /**
    * Reads a Parquet Dataframe from ''source'', sets its schema as specified by ''schemaRetentionPolicy'' and returns it.
    */
  private def readParquetAsAvro(source: String, schemaRetentionPolicy: SchemaRetentionPolicy, spark: SparkSession): Dataset[Row] = {

    import za.co.absa.abris.avro.AvroSerDe._

    spark
      .read
      .parquet(source)
      // this option will extract the Avro record from "value" and make its schema the schema for the dataframe
      .fromAvro("value", AvroSchemaUtils.load(AVRO_SCHEMA))(schemaRetentionPolicy)
  }
  
  private def loadProperties(path: String): Properties = {
    val properties = new Properties()
    properties.load(new FileInputStream(path))
    properties
  }

  private def getRows(howMany: Int): List[Row] = {
    ComplexRecordsGenerator.generateUnparsedRows(howMany)
  }

  private def getEncoder(): Encoder[Row] = {
    val avroSchema = AvroSchemaUtils.load(AVRO_SCHEMA)
    val sparkSchema = SparkAvroConversions.toSqlType(avroSchema)
    RowEncoder.apply(sparkSchema)
  }        
}