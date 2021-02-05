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

import java.nio.ByteBuffer
import org.apache.avro.Schema
import org.apache.avro.generic.GenericDatumReader
import org.apache.avro.io.{BinaryDecoder, DecoderFactory}
import org.apache.kafka.common.errors.SerializationException
import org.apache.spark.SparkException
import org.apache.spark.sql.avro.SchemaConverters
import org.apache.spark.sql.catalyst.expressions.codegen.{CodeGenerator, CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.expressions.{ExpectsInputTypes, Expression, UnaryExpression}
import org.apache.spark.sql.types.{BinaryType, DataType}
import za.co.absa.abris.avro.parsing.utils.AvroSchemaUtils
import za.co.absa.abris.avro.read.confluent.{ConfluentConstants, SchemaManagerFactory}
import za.co.absa.abris.config.FromAvroConfig

import scala.collection.mutable
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

case class AvroDataToCatalyst(
   child: Expression,
   config: FromAvroConfig)
  extends UnaryExpression with ExpectsInputTypes {

  override def inputTypes: Seq[BinaryType.type] = Seq(BinaryType)

  override lazy val dataType: DataType = SchemaConverters.toSqlType(avroSchema).dataType

  override def nullable: Boolean = true

  private val confluentCompliant = config.schemaRegistryConf.isDefined

  @transient private lazy val schemaManager = SchemaManagerFactory.create(config.schemaRegistryConf.get)

  // Avro schema to be used for reading
  @transient private lazy val avroSchema = AvroSchemaUtils.parse(config.schemaString)

  // Avro schema that was used for data serialization
  @transient private lazy val writerSchema = config.writerSchema.map(s => AvroSchemaUtils.parse(s))

  // Reused GenericDatumReaders per writer schema
  @transient private lazy val vanillaReader: GenericDatumReader[Any] = new GenericDatumReader[Any](writerSchema.getOrElse(avroSchema), avroSchema)
  @transient private lazy val confluentReaderCache: mutable.HashMap[Int, GenericDatumReader[Any]] = new mutable.HashMap[Int, GenericDatumReader[Any]]()

  @transient private var decoder: BinaryDecoder = _

  @transient private lazy val deserializer = new AvroDeserializer(avroSchema, dataType)

  // Reused result object (usually of type IndexedRecord)
  @transient private var result: Any = _

  override def nullSafeEval(input: Any): Any = {
    val binary = input.asInstanceOf[Array[Byte]]
    try {
      val intermediateData = decode(binary)

      deserializer.deserialize(intermediateData)

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
    defineCodeGen(ctx, ev, input =>
      s"(${boxedType(ctx, dataType)})$expr.nullSafeEval($input)")
  }

  /**
   * The method boxedType(...) is placed in different classes in Spark 2.3 and 2.4
   */
  private def boxedType(ctx: CodegenContext, dataType: DataType): String = {
    val tryBoxedTypeSpark2_4 = Try {
      CodeGenerator
        .getClass
        .getMethod("boxedType", classOf[DataType])
        .invoke(CodeGenerator, dataType)
    }

    val boxedType = tryBoxedTypeSpark2_4.getOrElse {
      classOf[CodegenContext]
        .getMethod("boxedType", classOf[DataType])
        .invoke(ctx, dataType)
    }

    boxedType.asInstanceOf[String]
  }

  private def decode(payload: Array[Byte]): Any = if (confluentCompliant) {
    decodeConfluentAvro(payload)
  } else {
    decodeVanillaAvro(payload)
  }

  private def decodeConfluentAvro(payload: Array[Byte]): Any  = {

    val buffer = ByteBuffer.wrap(payload)
    if (buffer.get() != ConfluentConstants.MAGIC_BYTE) {
      throw new SerializationException("Unknown magic byte!")
    }

    val schemaId = buffer.getInt()

    val start = buffer.position() + buffer.arrayOffset()
    val length = buffer.limit() - 1 - ConfluentConstants.SCHEMA_ID_SIZE_BYTES
    decoder = DecoderFactory.get().binaryDecoder(buffer.array(), start, length, decoder)

    val reader = confluentReaderCache.getOrElseUpdate(schemaId, {
      val writerSchema = getWriterSchema(schemaId)
      new GenericDatumReader[Any](writerSchema, avroSchema)
    })

    result = reader.read(result, decoder)
    result
  }

  private def getWriterSchema(id: Int): Schema = {
    Try(schemaManager.getSchemaById(id)) match {
      case Success(schema)  => schema
      case Failure(e)       => throw new RuntimeException("Not able to load writer schema", e)
    }
  }

  private def decodeVanillaAvro(payload: Array[Byte]): Any = {

    decoder = DecoderFactory.get().binaryDecoder(payload, 0, payload.length, decoder)
    result = vanillaReader.read(result, decoder)
    result
  }
}
