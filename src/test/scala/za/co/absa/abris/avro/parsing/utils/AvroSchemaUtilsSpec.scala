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

package za.co.absa.abris.avro.parsing.utils

import org.apache.avro.Schema.Type
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, struct}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers


class AvroSchemaUtilsSpec extends AnyFlatSpec with Matchers {

  private val spark = SparkSession
    .builder()
    .appName("unitTest")
    .master("local[2]")
    .config("spark.driver.bindAddress", "localhost")
    .config("spark.ui.enabled", "false")
    .getOrCreate()

  import spark.implicits._


  val dataFrame = Seq((1, "bat", true), (2, "mouse", false)).toDF("number", "word", "bool")


  behavior of "AvroSchemaUtils"

  it should "convert the schema of whole dataframe" in {

    val schema = AvroSchemaUtils.toAvroSchema(dataFrame)

    schema.getType shouldBe Type.RECORD
    schema.getFullName shouldBe "topLevelRecord"
    schema.getFields.get(0).schema().getType shouldBe Type.INT
    schema.getFields.get(1).schema().getType shouldBe Type.UNION
    schema.getFields.get(2).schema().getType shouldBe Type.BOOLEAN


    val schema2 = AvroSchemaUtils.toAvroSchema(dataFrame, "foo", "bar")
    schema2.getType shouldBe Type.RECORD
    schema2.getFullName shouldBe "bar.foo"
  }

  it should "convert the schema of multiple selected columns" in {

    val schema = AvroSchemaUtils.toAvroSchema(dataFrame, Seq("bool","number"))

    schema.getType shouldBe Type.RECORD
    schema.getFullName shouldBe "topLevelRecord"
    schema.getFields.size() shouldBe 2
    schema.getFields.get(0).schema().getType shouldBe Type.BOOLEAN
    schema.getFields.get(1).schema().getType shouldBe Type.INT


    val schema2 = AvroSchemaUtils.toAvroSchema(dataFrame, Seq("bool","number"), "foo", "bar")
    schema2.getType shouldBe Type.RECORD
    schema2.getFullName shouldBe "bar.foo"
  }

  it should "convert the schema of one selected simple column" in {

    val schema = AvroSchemaUtils.toAvroSchema(dataFrame, "bool")

    schema.getType shouldBe Type.BOOLEAN
    schema.getFullName shouldBe "boolean"

    val schema2 = AvroSchemaUtils.toAvroSchema(dataFrame, "bool", "foo", "bar")
    schema2.getType shouldBe Type.BOOLEAN
    schema2.getFullName shouldBe "boolean"
  }

  it should "convert the schema of one selected struct column" in {

    val structDataFrame = dataFrame.select(struct(col("bool"), col("number")) as "str")

    val schema = AvroSchemaUtils.toAvroSchema(structDataFrame, "str")

    schema.getType shouldBe Type.RECORD
    schema.getFullName shouldBe "topLevelRecord"
    schema.getFields.size() shouldBe 2
    schema.getFields.get(0).schema().getType shouldBe Type.BOOLEAN
    schema.getFields.get(1).schema().getType shouldBe Type.INT

    val schema2 = AvroSchemaUtils.toAvroSchema(structDataFrame, "str", "foo", "bar")

    schema2.getType shouldBe Type.RECORD
    schema2.getFullName shouldBe "bar.foo"
    schema2.getFields.size() shouldBe 2
    schema2.getFields.get(0).schema().getType shouldBe Type.BOOLEAN
    schema2.getFields.get(1).schema().getType shouldBe Type.INT
  }

}
