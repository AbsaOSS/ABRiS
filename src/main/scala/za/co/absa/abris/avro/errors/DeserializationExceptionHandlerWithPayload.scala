/*
 * Copyright 2024 ABSA Group Limited
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
import org.apache.spark.sql.avro.AbrisAvroDeserializer

/**
 * Extended deserialization exception handler that also receives the raw Avro payload
 * (as `Array[Byte]`) when deserialization fails.
 *
 * This is useful for scenarios such as:
 *  - Persisting corrupted raw messages to a Dead Letter Queue (DLQ)
 *  - Auditing failed records
 *  - Storing raw Avro bytes for regulatory/compliance use cases
 *  - Implementing advanced quarantine logic
 *
 * Implementations must also provide the inherited [[DeserializationExceptionHandler.handle]] method
 * as a fallback. When used with ABRiS, [[handleWithPayload]] takes precedence over [[handle]].
 */
trait DeserializationExceptionHandlerWithPayload extends DeserializationExceptionHandler {

  /**
   * Handle a deserialization failure with access to the raw binary payload.
   *
   * @param exception    the exception that occurred during deserialization
   * @param deserializer the Avro deserializer instance
   * @param readerSchema the Avro reader schema
   * @param payload      the raw binary payload that failed to deserialize
   * @return a value to use in place of the failed record (e.g. a null-filled row)
   */
  def handleWithPayload(
    exception: Throwable,
    deserializer: AbrisAvroDeserializer,
    readerSchema: Schema,
    payload: Array[Byte]
  ): Any

}
