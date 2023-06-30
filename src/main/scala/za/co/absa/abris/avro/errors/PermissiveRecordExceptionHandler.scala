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

package za.co.absa.abris.avro.errors

import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.avro.specific.SpecificRecordBase
import org.apache.spark.internal.Logging
import org.apache.spark.sql.avro.AbrisAvroDeserializer

class PermissiveRecordExceptionHandler() extends DeserializationExceptionHandler with Logging {

  def handle(exception: Throwable, deserializer: AbrisAvroDeserializer, readerSchema: Schema): Any = {
    logWarning("Malformed record detected. Replacing with full null row.", exception)
    val record = new GenericData.Record(readerSchema)
    deserializer.deserialize(record)
  }
}
