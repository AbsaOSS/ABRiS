/*
 * Copyright 2021 ABSA Group Limited
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

package za.co.absa.abris.avro.sql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.BeforeAndAfterEach
import org.scalatest.matchers.should.Matchers
import za.co.absa.abris.avro.functions._
import za.co.absa.abris.config.{AbrisConfig, FromAvroConfig}
import za.co.absa.abris.examples.data.generation.TestSchemas

class AvroDataToCatalystSpec extends AnyFlatSpec with Matchers with BeforeAndAfterEach {
  private val spark = SparkSession
    .builder()
    .appName("unitTest")
    .master("local[2]")
    .config("spark.driver.bindAddress", "localhost")
    .config("spark.ui.enabled", "false")
    .getOrCreate()

  import spark.implicits._


  it should "not print schema registry configs in the spark plan" in {
    val sensitiveData = "username:password"
    val schemaString = TestSchemas.NATIVE_SIMPLE_NESTED_SCHEMA
    val dummyUrl = "dummyUrl"

    val fromAvroConfig = FromAvroConfig()
      .withReaderSchema(schemaString)
      .withSchemaRegistryConfig(Map(
        AbrisConfig.SCHEMA_REGISTRY_URL -> dummyUrl,
        "basic.auth.user.info" -> sensitiveData
      ))

    val column = from_avro(col("avroBytes"), fromAvroConfig)
    column.expr.toString() should not include sensitiveData
  }

}
