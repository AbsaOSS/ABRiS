/*
 * Copyright 2019 ABSA Group Limited
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

import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.functions.struct
import org.apache.spark.sql.{DataFrame, Encoder, Row, SparkSession}
import za.co.absa.abris.avro.format.SparkAvroConversions
import za.co.absa.abris.avro.parsing.utils.AvroSchemaUtils
import za.co.absa.abris.config.AbrisConfig
import za.co.absa.abris.examples.data.generation.ComplexRecordsGenerator


object ConfluentKafkaAvroWriter {

  val kafkaTopicName = "test_topic"

  val dummyDataRows = 5
  val dummyDataPartitions = 1

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("ReaderJob")
      .master("local[2]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("INFO")

    val dataFrame = generateRandomDataFrame(spark)

    dataFrame.show(false)

    val schemaString = ComplexRecordsGenerator.usedAvroSchema

    // to serialize all columns in dataFrame we need to put them in a spark struct
    val allColumns = struct(dataFrame.columns.head, dataFrame.columns.tail: _*)

    val abrisConfig = AbrisConfig
      .toConfluentAvro
      .provideAndRegisterSchema(schemaString)
      .usingTopicNameStrategy(kafkaTopicName)
      .usingSchemaRegistry("http://localhost:8081")

    import za.co.absa.abris.avro.functions.to_avro

    val avroFrame = dataFrame.select(to_avro(allColumns, abrisConfig) as 'value)

    avroFrame
      .write
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("topic", kafkaTopicName)
      .save()
  }

  private def generateRandomDataFrame(spark: SparkSession): DataFrame = {
    import spark.implicits._

    implicit val encoder: Encoder[Row] = getEncoder

    val rows = createRows(dummyDataRows)
    spark.sparkContext.parallelize(rows, dummyDataPartitions).toDF()
  }

  private def createRows(howMany: Int): List[Row] = {
    ComplexRecordsGenerator.generateUnparsedRows(howMany)
  }

  private def getEncoder: Encoder[Row] = {
    val avroSchema = AvroSchemaUtils.parse(ComplexRecordsGenerator.usedAvroSchema)
    val sparkSchema = SparkAvroConversions.toSqlType(avroSchema)
    RowEncoder.apply(sparkSchema)
  }
}
