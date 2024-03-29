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

package za.co.absa.abris.avro.schemas

import org.apache.commons.io.FileUtils
import org.scalatest.flatspec.AnyFlatSpec
import za.co.absa.abris.avro.parsing.utils.AvroSchemaUtils
import za.co.absa.abris.examples.data.generation.TestSchemas

import java.io.File
import java.nio.charset.Charset

class SchemaLoaderSpec extends AnyFlatSpec {

  private val testDir = new File("testDirSchemaLoader")

  behavior of "SchemaLoader"

  it should "retrieve schemas from file systems" in {
    val expectedSchemaString = TestSchemas.COMPLEX_SCHEMA_SPEC
    val expectedSchema = AvroSchemaUtils.parse(expectedSchemaString)
    val schemaFileName = "testSchemaName"
    val destination = writeIntoFS(expectedSchemaString, schemaFileName)
    val loadedSchema = AvroSchemaUtils.load(destination.getAbsolutePath)

    FileUtils.deleteQuietly(new File(destination.getAbsolutePath))
    FileUtils.deleteDirectory(testDir)

    assert(expectedSchema.equals(loadedSchema))
  }

  private def writeIntoFS(schema: String, name: String): File = {
    val destination = new File(testDir, name)
    FileUtils.write(destination, schema, Charset.defaultCharset)
    destination
  }
}
