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

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import za.co.absa.abris.config.ToAvroConfig.Key

class ToAvroConfigSpec extends AnyFlatSpec with Matchers {

  behavior of "ToAvroConfig"

  it should "provide map with all set configurations" in {
    val config = ToAvroConfig()
      .withSchema("foo")
      .withSchemaId(42)

    val map = config.abrisConfig()
    map(Key.Schema) shouldBe "foo"
    map(Key.SchemaId) shouldBe 42
  }

  it should "support the legacy constructor and methods" in {
    val config = new ToAvroConfig("foo", Some(2))

    config.schemaString() shouldBe "foo"
    config.schemaId() shouldBe Some(2)

    val map = config.abrisConfig()
    map(Key.Schema) shouldBe "foo"
    map(Key.SchemaId) shouldBe 2
  }

  it should "throw when validation fails" in {
    val config = ToAvroConfig()

    val thrown = intercept[IllegalArgumentException] {
      config.validate()
    }
    thrown.getMessage.contains(Key.Schema) shouldBe true
  }

}
