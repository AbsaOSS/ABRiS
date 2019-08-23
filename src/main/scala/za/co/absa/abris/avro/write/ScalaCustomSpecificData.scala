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

package za.co.absa.abris.avro.write

import org.apache.avro.generic.GenericData.Record
import org.apache.avro.generic.IndexedRecord
import org.apache.avro.{Schema, UnresolvedUnionException}
import org.apache.spark.sql.catalyst.expressions.GenericRow

import scala.util.Try


object ScalaCustomSpecificData {
  private val INSTANCE = new ScalaCustomSpecificData()
  def get(): ScalaCustomSpecificData = INSTANCE
}

/**
 * This class redefines the way fields are retrieved from a record since they could be either
 * an IndexedRecord (if written by Avro library) or a GenericRow, if written this library.
 */
class ScalaCustomSpecificData extends za.co.absa.abris.avro.format.ScalaSpecificData {
    
  override def getField(record: Object, name: String, position: Int): Object = {
    try {
      record.asInstanceOf[IndexedRecord].get(position)
    }
    catch {
      case _: Throwable => record.asInstanceOf[GenericRow].get(position).asInstanceOf[Object] 
    }
  }

  /**
    * This is a hack. When a Record datum is created from a dataframe its schema namespace is defined by the location
    * of the attribute in the dataframe schema as opposed to the avro schema that is to be imposed. This means when the
    * logic tries to resolve the namespace of the datum (record) from the given types in the union it cannot find it.
    * The below logic works by if all else fails trying to match the datum schema name with any of the union type names
    * and if found returning that.
    * @param union schema
    * @param datum data
    */
  override def resolveUnion(union: Schema, datum: Object): Int = {
    Try(super.resolveUnion(union, datum)).getOrElse {
      val name = datum.asInstanceOf[Record].getSchema.getName.toLowerCase
      val maybeIndex = Range(0, union.getTypes.size()).find(x => name.equals(union.getTypes.get(x).getName.toLowerCase))
      maybeIndex.getOrElse(throw new UnresolvedUnionException(union, datum))
    }
  }
}