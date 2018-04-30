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

package za.co.absa.abris.examples.data.generation

import org.apache.avro.generic.GenericFixed

object FixedString {
  def getClassName() = new FixedString("").getClass.getName
}

/**
 * Utility class for writing Avro fixed fields.
 */
class FixedString(value: String) extends GenericFixed {
  override def getSchema() = null
  override def bytes() = value.getBytes
}