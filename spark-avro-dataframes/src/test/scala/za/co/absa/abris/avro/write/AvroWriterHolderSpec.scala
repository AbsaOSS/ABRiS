/*
 * Copyright 2018 Barclays Africa Group Limited
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

package za.co.absa.abris.avro.write

import org.scalatest.FlatSpec
import java.io.ByteArrayOutputStream

import za.co.absa.abris.avro.parsing.utils.AvroSchemaUtils
import za.co.absa.abris.examples.data.generation.TestSchemas

class AvroWriterHolderSpec extends FlatSpec {
  
  behavior of "AvroWriterHolder"
  
  it should "reuse Encoders" in {    
    val holder = new AvroWriterHolder()
    val encoder = holder.getEncoder(new ByteArrayOutputStream())
    assert(encoder == holder.getEncoder(new ByteArrayOutputStream()))
  }
}