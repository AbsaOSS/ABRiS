/*
 * Copyright 2022 ABSA Group Limited
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

import org.apache.avro.SchemaBuilder
import org.apache.spark.SparkConf
import org.apache.spark.serializer.{JavaSerializer, KryoSerializer}
import org.apache.spark.sql.functions.col
import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import za.co.absa.abris.avro.functions._
import za.co.absa.abris.config.ToAvroConfig

class CatalystDataToAvroSpec extends AnyFlatSpec with Matchers with BeforeAndAfterEach {
  it should "be serializable" in {
    val schema = SchemaBuilder
      .record("foo")
      .namespace("test_namespace")
      .fields()
      .name("int").`type`().intType().noDefault()
      .endRecord()
      .toString
    val config = ToAvroConfig().withSchema(schema)
    val catalystDataToAvro = to_avro(col("col"), config).expr

    val javaSerializer = new JavaSerializer(new SparkConf())
    javaSerializer.newInstance().serialize(catalystDataToAvro)

    val kryoSerializer = new KryoSerializer(new SparkConf())
    kryoSerializer.newInstance().serialize(catalystDataToAvro)

    // test successful if no exception is thrown
  }
}
