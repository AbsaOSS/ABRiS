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

import java.security.InvalidParameterException

import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, IndexedRecord}
import org.apache.spark.sql.avro.AvroSerializer
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.expressions.{Expression, UnaryExpression}
import org.apache.spark.sql.types.{BinaryType, DataType}
import za.co.absa.abris.avro.format.SparkAvroConversions
import za.co.absa.abris.avro.parsing.utils.AvroSchemaUtils
import za.co.absa.abris.avro.read.confluent.SchemaManager

case class CatalystDataToAvro(
   child: Expression,
   schemaProvider: SchemaProvider,
   schemaRegistryConf: Option[Map[String,String]],
   confluentCompliant: Boolean)
  extends UnaryExpression {

  override def dataType: DataType = BinaryType

  @transient private lazy val serializer: AvroSerializer =
    new AvroSerializer(child.dataType, schemaProvider.originalSchema(child), child.nullable)

  override def nullSafeEval(input: Any): Any = {
    val avroData = serializer.serialize(input)

    val schemaId = schemaRegistryConf.flatMap(conf =>
      registerSchema(schemaProvider.wrappedSchema(child), conf, confluentCompliant))

    val record : IndexedRecord = avroData match {
      case ad: IndexedRecord => ad
      case _ => wrapWithRecord(avroData)
    }

    SparkAvroConversions.toByteArray(record, record.getSchema, schemaId)
  }

  private def wrapWithRecord(avroData:Any) = {
    val record = new GenericData.Record(schemaProvider.wrappedSchema(child))
    record.put(0, avroData)
    record
  }

  override def prettyName: String = "to_avro"

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val expr = ctx.addReferenceObj("this", this)
    defineCodeGen(ctx, ev, input =>
      s"(byte[]) $expr.nullSafeEval($input)")
  }

  /**
   * Tries to manage schema registration in case credentials to access Schema Registry are provided.
   */
  @throws[InvalidParameterException]
  private def registerSchema(
      schema: Schema,
      registryConfig: Map[String,String],
      prependSchemaId: Boolean): Option[Int] = {

    val topic = registryConfig(SchemaManager.PARAM_SCHEMA_REGISTRY_TOPIC)

    val valueStrategy = registryConfig.get(SchemaManager.PARAM_VALUE_SCHEMA_NAMING_STRATEGY)
    val keyStrategy = registryConfig.get(SchemaManager.PARAM_KEY_SCHEMA_NAMING_STRATEGY)

    val schemaId = (valueStrategy, keyStrategy) match {
      case (Some(valueStrategy), None) => AvroSchemaUtils.registerIfCompatibleValueSchema(topic, schema, registryConfig)
      case (None, Some(keyStrategy)) => AvroSchemaUtils.registerIfCompatibleKeySchema(topic, schema, registryConfig)
      case (Some(_), Some(_)) =>
        throw new InvalidParameterException(
          "Both key.schema.naming.strategy and value.schema.naming.strategy were defined. " +
            "Only one of them supposed to be defined!")
      case _ =>
        throw new InvalidParameterException(
          "At least one of key.schema.naming.strategy or value.schema.naming.strategy " +
            "must be defined to use schema registry!")
    }

    if (schemaId.isEmpty) {
      throw new InvalidParameterException(s"Schema could not be registered for topic '$topic'. " +
        "Make sure that the Schema Registry is available, the parameters are correct and the schemas ar compatible")
    }

    if (prependSchemaId) schemaId else None
  }

}
