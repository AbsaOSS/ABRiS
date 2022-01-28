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

import org.apache.avro.Schema
import org.apache.spark.sql.types.LongType
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import za.co.absa.abris.config.FromAvroConfig.Key

class FromAvroConfigSpec extends AnyFlatSpec with Matchers {

  behavior of "FromAvroConfig"

  it should "provide all set configurations" in {
    val dummySchemaConverter = (_: Schema) => LongType
    val config = FromAvroConfig()
      .withWriterSchema("foo")
      .withReaderSchema("bar")
      .withSchemaConverter(dummySchemaConverter)
      .withSchemaRegistryConfig(Map(AbrisConfig.SCHEMA_REGISTRY_URL -> "url"))

    val map = config.abrisConfig()
    map(Key.WriterSchema) shouldBe "foo"
    map(Key.ReaderSchema) shouldBe "bar"
    map(Key.SchemaConverter) shouldBe dummySchemaConverter

    config.schemaRegistryConf().get(AbrisConfig.SCHEMA_REGISTRY_URL) shouldBe "url"
  }

  it should "support the legacy constructor and methods" in {
    val config = new FromAvroConfig("foo", Some(Map(AbrisConfig.SCHEMA_REGISTRY_URL -> "url")))

    config.schemaString() shouldBe "foo"
    config.schemaRegistryConf().get(AbrisConfig.SCHEMA_REGISTRY_URL) shouldBe "url"

    val map = config.abrisConfig()
    map(Key.ReaderSchema) shouldBe "foo"
  }

  it should "throw when validation fails" in {
    val config = FromAvroConfig()

    val thrown = intercept[IllegalArgumentException] {
      config.validate()
    }
    thrown.getMessage.contains(Key.ReaderSchema) shouldBe true

    val config2 = FromAvroConfig()
      .withWriterSchema("foo")
      .withReaderSchema("bar")
      .withSchemaRegistryConfig(Map(AbrisConfig.SCHEMA_REGISTRY_URL -> "url"))

    val thrown2 = intercept[IllegalArgumentException] {
      config2.validate()
    }
    thrown2.getMessage.contains(Key.WriterSchema) shouldBe true
  }
}
