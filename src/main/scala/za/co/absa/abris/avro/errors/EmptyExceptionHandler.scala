/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package za.co.absa.abris.avro.errors

import org.apache.avro.Schema
import org.apache.avro.hadoop.io.AvroDeserializer
import org.apache.spark.internal.Logging
import org.apache.spark.sql.avro.AbrisAvroDeserializer
import org.apache.spark.sql.types.DataType

class EmptyExceptionHandler extends DeserializationExceptionHandler with Logging with Serializable {

  def handle(exception: Throwable, deserializer: AbrisAvroDeserializer, readerSchema: Schema, dataType: DataType): Any = {
    logWarning("NullExceptionHandler", exception)
    deserializer.deserialize(new ScalaAvroRecord(readerSchema))
    logWarning("successfully handle exception")
  }
}
