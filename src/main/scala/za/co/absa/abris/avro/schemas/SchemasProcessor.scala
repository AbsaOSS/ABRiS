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

package za.co.absa.abris.avro.schemas

import org.apache.avro.Schema
import org.apache.spark.sql.types.StructType

/**
 * Trait for objects that can produce Avro and Spark schemas from each other.
 */
trait SchemasProcessor extends Serializable {  
  def getAvroSchema(): Schema
  def getSparkSchema(): StructType
}