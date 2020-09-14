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

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import za.co.absa.abris.config.AbrisConfig

object ConfluentKafkaAvroReader {

  val kafkaTopicName = "test_topic"

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("WriterJob")
      .master("local[2]")
      .getOrCreate()


    spark.sparkContext.setLogLevel("INFO")

    val dataFrame = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", kafkaTopicName)
      .option("startingOffsets", "earliest")
      .load()

    val abrisConfig = AbrisConfig
      .fromConfluentAvro
      .downloadReaderSchemaByLatestVersion
      .andTopicNameStrategy(kafkaTopicName)
      .usingSchemaRegistry("http://localhost:8081")

    import za.co.absa.abris.avro.functions.from_avro
    val deserialized = dataFrame.select(from_avro(col("value"), abrisConfig) as 'data)

    deserialized.printSchema()

    deserialized
      .writeStream
      .format("console")
      .option("truncate", "false")
      .start()
      .awaitTermination()
  }
}
