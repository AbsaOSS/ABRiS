/*
 * Copyright 2020 ABSA Group Limited
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

import org.apache.avro.Schema
import za.co.absa.abris.avro.parsing.utils.AvroSchemaUtils
import za.co.absa.abris.config.ToAvroConfig.Key

private[abris] class InternalToAvroConfig(map: Map[String, Any]) {

  def schema: Schema = AvroSchemaUtils.parse(map(Key.Schema).asInstanceOf[String])

  def schemaId: Option[Int] = map.get(Key.schemaId).map(_.asInstanceOf[Int])
}
