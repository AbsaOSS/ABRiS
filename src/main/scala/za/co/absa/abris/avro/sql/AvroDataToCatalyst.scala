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

package za.co.absa.abris.avro.sql

import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.spark.SparkException
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.expressions.{ExpectsInputTypes, Expression, UnaryExpression}
import org.apache.spark.sql.types.{BinaryType, DataType}
import za.co.absa.abris.avro.format.SparkAvroConversions
import za.co.absa.abris.avro.parsing.utils.AvroSchemaUtils
import za.co.absa.abris.avro.read.confluent.{ScalaConfluentKafkaAvroDeserializer, SchemaManager}
import za.co.absa.abris.avro.serde.{AvroReaderFactory, AvroToRowConverter}

import scala.util.control.NonFatal

case class AvroDataToCatalyst(
    child: Expression,
    jsonFormatSchema: Option[String],
    schemaRegistryConf: Option[Map[String,String]],
    removeSchemaId: Boolean)
  extends UnaryExpression with ExpectsInputTypes {

  override def inputTypes: Seq[BinaryType.type] = Seq(BinaryType)

  override lazy val dataType: DataType = SparkAvroConversions.toSqlType(avroSchema)

  override def nullable: Boolean = true

  @transient private lazy val avroSchema = (jsonFormatSchema, schemaRegistryConf) match {
    case (Some(schemaString), _) => new Schema.Parser().parse(schemaString)
    case (_, Some(schemaRegistryConf)) => AvroSchemaUtils.load(schemaRegistryConf)
    case _ => throw new SparkException("Schema or schema registry configuration must be provided")
  }

  override def nullSafeEval(input: Any): Any = {
    val binary = input.asInstanceOf[Array[Byte]]
    try {
      val genericRecord = convertToGenericRecord(binary)

      val deserializer = new AvroDeserializer(avroSchema, dataType)
      deserializer.deserialize(genericRecord)

    } catch {
      // There could be multiple possible exceptions here, e.g. java.io.IOException,
      // AvroRuntimeException, ArrayIndexOutOfBoundsException, etc.
      // To make it simple, catch all the exceptions here.
      case NonFatal(e) =>  throw new SparkException("Malformed records are detected in record parsing.", e)
    }
  }

  override def prettyName: String = "from_avro"

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val expr = ctx.addReferenceObj("this", this)
    nullSafeCodeGen(ctx, ev, eval => {
      val result = ctx.freshName("result")
      val dt = ctx.boxedType(dataType)
      s"""
        $dt $result = ($dt) $expr.nullSafeEval($eval);
        if ($result == null) {
          ${ev.isNull} = true;
        } else {
          ${ev.value} = $result;
        }
      """
    })
  }

  def convertToGenericRecord(bytes: Array[Byte]): GenericRecord = {
    if (removeSchemaId) {
      createConfiguredConfluentAvroReader(Option(avroSchema), schemaRegistryConf).deserialize(bytes)
    } else {
      val avroToRowConverter = new AvroToRowConverter(Some(AvroReaderFactory.createAvroReader(avroSchema)))
      avroToRowConverter.convertToGenericRecord(bytes)
    }
  }

  /**
   * Creates an instance of [[ScalaConfluentKafkaAvroDeserializer]] and configures its Schema Registry access in case
   * the parameters to do it are defined.
   */
  def createConfiguredConfluentAvroReader(schema: Option[Schema], schemaRegistryConf: Option[Map[String,String]]):
      ScalaConfluentKafkaAvroDeserializer = {

    val configs = schemaRegistryConf.getOrElse(Map[String,String]())
    val topic = configs.get(SchemaManager.PARAM_SCHEMA_REGISTRY_TOPIC)

    val reader = new ScalaConfluentKafkaAvroDeserializer(topic, schema)
    reader.configureSchemaRegistry(configs)
    reader
  }

}
