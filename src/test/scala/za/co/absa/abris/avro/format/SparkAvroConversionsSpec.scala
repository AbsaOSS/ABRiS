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

package za.co.absa.abris.avro.format

import org.apache.avro.Schema.Type
import org.apache.avro.SchemaBuilder
import org.apache.spark.sql.types._
import org.scalatest.{FlatSpec, Matchers}
import za.co.absa.abris.avro.parsing.utils.AvroSchemaUtils
import za.co.absa.abris.examples.data.generation.TestSchemas

import scala.collection.JavaConverters._
import scala.collection._

class SparkAvroConversionsSpec extends FlatSpec with Matchers {

  // scalastyle:off magic.number

  private val structType = StructType(
      Seq(
        StructField("int1", IntegerType, false),
        StructField("long1", LongType, false),
        StructField("map1", new MapType(StringType, IntegerType, false), false),
        StructField("array1", new ArrayType(LongType, false), false),
        StructField("struct2", StructType(
          Seq(
            StructField("string2", StringType, true),
            StructField("string3", StringType, false)
          )
        ), false),
        StructField("double1", DoubleType, false),
        StructField("struct3", StructType(
          Seq(
            StructField("int2", IntegerType, false),
            StructField("float1", FloatType, false)
          )
        ), false),
        StructField("bytes", BinaryType, false)
      )
    )

  behavior of "SparkAvroConversions"

  it should "convert Avro schemas to SQL types" in {
    val schema = AvroSchemaUtils.parse(TestSchemas.COMPLEX_SCHEMA_SPEC)
    val sql = SparkAvroConversions.toSqlType(schema)
    val schemaFromSql = SparkAvroConversions.toAvroSchema(sql, schema.getName, schema.getNamespace)

    schema.getFields.asScala.foreach(field =>
      assert(schema.getField(field.name).toString == schemaFromSql.getField(field.name).toString))
  }

  it should "convert SQL types to Avro schemas" in {
    val schemaName = "teste_name"
    val schemaNamespace = "teste_namespace"

    val schema = SparkAvroConversions.toAvroSchema(structType, schemaName, schemaNamespace)

    assert(schema.getName == schemaName)
    assert(schema.getNamespace == schemaNamespace)
    assert(schema.getField("int1").schema().getType == Type.INT)
    assert(schema.getField("long1").schema().getType == Type.LONG)
    assert(schema.getField("map1").schema().getType == Type.MAP)
    assert(schema.getField("array1").schema().getType == Type.ARRAY)
    assert(schema.getField("struct2").schema().getType == Type.RECORD)
    assert(schema.getField("double1").schema().getType == Type.DOUBLE)
    assert(schema.getField("struct3").schema().getType == Type.RECORD)
    assert(schema.getField("bytes").schema().getType == Type.BYTES)

    val map1 = schema.getField("map1").schema()
    assert(map1.getValueType.getType == Type.INT)

    val array1 = schema.getField("array1").schema()
    assert(array1.getElementType.getType == Type.LONG)

    val struct2 = schema.getField("struct2").schema()
    assert(struct2.getField("string2").schema().getType == Type.UNION) // nullable fields are "unioned" with null
    assert(struct2.getField("string3").schema().getType == Type.STRING)

    val struct3 = schema.getField("struct3").schema()
    assert(struct3.getField("int2").schema().getType == Type.INT)
    assert(struct3.getField("float1").schema().getType == Type.FLOAT)
  }

  it should "convert fixed and bytes type" in {

    val avroSchema = SchemaBuilder
      .record("test_record")
      .namespace("test_namespace")
      .fields()
        .name("fixed_name").`type`().fixed("fixed_name").size(3).noDefault()
        .name("bytes_name").`type`().bytesType().noDefault()
      .endRecord()

    val sparkSchema = SparkAvroConversions.toSqlType(avroSchema)

    sparkSchema.fields(0) shouldBe StructField("fixed_name", BinaryType, false)
    sparkSchema.fields(1) shouldBe StructField("bytes_name", BinaryType, false)
  }

  // scalastyle:on magic.number
}
