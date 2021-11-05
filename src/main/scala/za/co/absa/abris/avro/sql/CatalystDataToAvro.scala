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

import org.apache.avro.generic.GenericDatumWriter
import org.apache.avro.io.{BinaryEncoder, EncoderFactory}
import org.apache.spark.sql.avro.AbrisAvroSerializer
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.expressions.{Expression, UnaryExpression}
import org.apache.spark.sql.types.{BinaryType, DataType}
import za.co.absa.abris.avro.read.confluent.ConfluentConstants
import za.co.absa.abris.config.InternalToAvroConfig

import java.io.ByteArrayOutputStream
import java.nio.ByteBuffer

private[abris] case class CatalystDataToAvro(
  child: Expression,
  abrisConfig: Map[String,Any]
) extends UnaryExpression {

  override def dataType: DataType = BinaryType

  @transient private lazy val config = new InternalToAvroConfig(abrisConfig)

  @transient private lazy val serializer: AbrisAvroSerializer =
    new AbrisAvroSerializer(child.dataType, config.schema, child.nullable)

  @transient private lazy val writer =
    new GenericDatumWriter[Any](config.schema)

  @transient private var encoder: BinaryEncoder = _

  @transient private lazy val out = new ByteArrayOutputStream

  override def nullSafeEval(input: Any): Any = {
    out.reset()

    config.schemaId.foreach { id =>
      attachSchemaId(id, out)
    }

    encoder = EncoderFactory.get().directBinaryEncoder(out, encoder)
    val avroData = serializer.serialize(input)
    writer.write(avroData, encoder)
    encoder.flush()
    out.toByteArray
  }

  private def attachSchemaId(id: Int, outStream: ByteArrayOutputStream) = {
    outStream.write(ConfluentConstants.MAGIC_BYTE)
    outStream.write(ByteBuffer.allocate(ConfluentConstants.SCHEMA_ID_SIZE_BYTES).putInt(id).array())
  }

  override def prettyName: String = "to_avro"

  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val expr = ctx.addReferenceObj("this", this)
    defineCodeGen(ctx, ev, input =>
      s"(byte[]) $expr.nullSafeEval($input)")
  }
}
