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

package za.co.absa.abris.config

import org.apache.avro.SchemaBuilder
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class InternalFromAvroConfigSpec extends AnyFlatSpec with Matchers {

  import InternalFromAvroConfigSpec._

  behavior of "InternalFromAvroConfig"

  it should "convert and provide all set properties" in {
    val config = FromAvroConfig()
      .withReaderSchema(avroReaderSchema.toString)
      .withWriterSchema(avroWriterSchema.toString)

    val intConfig = new InternalFromAvroConfig(config.abrisConfig())

    val readerSchema = intConfig.readerSchema
    readerSchema.getName shouldBe "reader"
    readerSchema.getNamespace shouldBe "test_namespace"
    readerSchema.getFields.size() shouldBe 2

    val writerSchema = intConfig.writerSchema.get
    writerSchema.getName shouldBe "writer"
    writerSchema.getNamespace shouldBe "test_namespace"
    writerSchema.getFields.size() shouldBe 1
  }

  it should "return None for optional properties that were not set" in {
    val config = FromAvroConfig()
      .withReaderSchema(avroReaderSchema.toString)

    val intConfig = new InternalFromAvroConfig(config.abrisConfig())

    intConfig.writerSchema shouldBe None
  }
}

object InternalFromAvroConfigSpec {

  val avroReaderSchema = SchemaBuilder
    .record("reader")
    .namespace("test_namespace")
    .fields()
    .name("int").`type`().intType().noDefault()
    .name("bytes_name").`type`().stringType().noDefault()
    .endRecord()

  val avroWriterSchema = SchemaBuilder
    .record("writer")
    .namespace("test_namespace")
    .fields()
    .name("int").`type`().intType().noDefault()
    .endRecord()
}
