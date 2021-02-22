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
import org.scalatest.FlatSpec
import org.scalatest.matchers.should.Matchers

class InternalToAvroConfigSpec extends FlatSpec with Matchers {

  import InternalToAvroConfigSpec._

  behavior of "InternalToAvroConfig"

  it should "convert and provide all set properties" in {
    val config = ToAvroConfig()
      .withSchema(avroSchema.toString)
      .withSchemaId(42)

    val intConfig = new InternalToAvroConfig(config.abrisConfig())

    val schema = intConfig.schema
    schema.getName shouldBe "foo"
    schema.getNamespace shouldBe "test_namespace"
    schema.getFields.size() shouldBe 2

    intConfig.schemaId shouldBe Some(42)
  }

  it should "return None for optional properties that were not set" in {
    val config = ToAvroConfig()
      .withSchema(avroSchema.toString)

    val intConfig = new InternalToAvroConfig(config.abrisConfig())

    intConfig.schemaId shouldBe None
  }
}

object InternalToAvroConfigSpec {

  val avroSchema = SchemaBuilder
    .record("foo")
    .namespace("test_namespace")
    .fields()
    .name("int").`type`().intType().noDefault()
    .name("bytes_name").`type`().stringType().noDefault()
    .endRecord()
}

