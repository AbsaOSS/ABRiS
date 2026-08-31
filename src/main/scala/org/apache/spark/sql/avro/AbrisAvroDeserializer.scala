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

package org.apache.spark.sql.avro

import org.apache.avro.Schema
import org.slf4j.LoggerFactory
import org.apache.spark.sql.types.DataType
import za.co.absa.commons.annotation.DeveloperApi

/**
 * Simple wrapper to access spark package private class
 */
@DeveloperApi
class AbrisAvroDeserializer(rootAvroType: Schema, rootCatalystType: DataType) {
  private val logger = LoggerFactory.getLogger(this.getClass)
  private val deserializer: AvroDeserializer = new AvroDeserializer(rootAvroType, rootCatalystType,
    "LEGACY", false: java.lang.Boolean, "", -1)

  def deserialize(catalystData: Any): Any = {
    deserializer.deserialize(catalystData) match {
      case Some(value) => value
      case None =>
        logger.warn("Deserialization returned None. This may indicate a problem with the input data. Input data: "
          + catalystData)
        None
    }
  }
}
