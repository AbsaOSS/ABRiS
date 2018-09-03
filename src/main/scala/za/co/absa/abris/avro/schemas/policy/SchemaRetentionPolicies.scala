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

package za.co.absa.abris.avro.schemas.policy

/**
  * This object contains options for schema retention during Avro/Spark conversions.
  *
  * When [[za.co.absa.abris.avro.schemas.policy.SchemaRetentionPolicies.RETAIN_SELECTED_COLUMN_ONLY]] is used, the column
  * containing Avro data is extracted and the whole Dataframe schema becomes the Avro schema defining those data.
  *
  * When [[za.co.absa.abris.avro.schemas.policy.SchemaRetentionPolicies.RETAIN_ORIGINAL_SCHEMA]] is used, the current
  * Dataframe schema is kept, and the column containing the Avro data is decoded "in place", i.e. as a nested structure.
  *
  * A good example is a Kafka Dataframe. It contains fields such as ''key'', ''value'', ''partition'', etc. If the first option above
  * is used, the current schema is removed and the schema belonging to the ''value'' column is used, which means that the other
  * fields are ignored. If the second option is used, the original schema is kept, which means that the Avro record will be put
  * inside the ''value'' column.
  */
object SchemaRetentionPolicies {

  sealed trait SchemaRetentionPolicy

  // this option only retains the column containing the Avro data
  case object RETAIN_SELECTED_COLUMN_ONLY extends SchemaRetentionPolicy

  // this option retains the original schema and adds the Avro data to the corresponding column
  case object RETAIN_ORIGINAL_SCHEMA extends SchemaRetentionPolicy
}