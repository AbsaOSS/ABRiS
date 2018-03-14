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

package za.co.absa.avro.dataframes.avro.write

import org.apache.avro.generic.IndexedRecord
import org.apache.spark.sql.catalyst.expressions.GenericRow
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

object ScalaCustomSpecificData {
  private val INSTANCE = new ScalaCustomSpecificData()
  def get(): ScalaCustomSpecificData = INSTANCE
}

/**
 * This class redefines the way fields are retrieved from a record since they could be either
 * an IndexedRecord (if written by Avro library) or a GenericRow, if written this library.
 */
class ScalaCustomSpecificData extends za.co.absa.avro.dataframes.avro.format.ScalaSpecificData {
    
  override def getField(record: Object, name: String, position: Int): Object = {
    try {
      record.asInstanceOf[IndexedRecord].get(position)
    }
    catch {
      case _: Throwable => record.asInstanceOf[GenericRow].get(position).asInstanceOf[Object] 
    }
  }    
}