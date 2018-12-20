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

package za.co.absa.abris.avro.format



import org.apache.avro.Schema.{Field, Type}
import org.apache.avro.generic.GenericData.Fixed
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.avro.{AvroRuntimeException, Schema}
import org.apache.spark.sql.Row

import scala.collection._

/**
 * In order to support Spark-compliant queryable nested structures, nested Avro records need to be converted into Spark Rows.
 *
 * This class extends Avro's GenericRow to perform the conversion at read time.
 */
class ScalaAvroRecord(schema: Schema) extends GenericRecord with Comparable[ScalaAvroRecord] {

  private var values: Array[Object] = _

  if (schema == null || !Type.RECORD.equals(schema.getType())) {
    throw new AvroRuntimeException("Not a record schema: " + schema)
  }
  
  values = new Array[Object](schema.getFields().size())

  override def getSchema: Schema = {
    schema
  }

  override def put(key: String, value: Object): Unit = {
    val field: Schema.Field = schema.getField(key)
    if (field == null) {
      throw new AvroRuntimeException("Not a valid schema field: " + key)
    }
    put(field.pos(), value)
  }

  override def put(position: Int, value: Object): Unit = {
    values(position) = value match {
      case v: ScalaAvroRecord                                => toRow(value.asInstanceOf[ScalaAvroRecord].values)
      case v: java.nio.ByteBuffer                            => v.array()      
      case v: Fixed                                          => v.bytes()
      case v: mutable.ListBuffer[Any]  if isListOfRecords(v) => convertToListOfRows(v)
      case v: mutable.HashMap[Any,Any] if isMapOfRecords(v)  => convertToMapOfRows(v)      
      case default                                           => default
    }
  }  
  
  private def toRow(values: Array[Object]) = {
    if (values.length == 1) {
      Row(values(0))
    } else {
      Row.fromSeq(values.toSeq)
    }
  }

  private def convertToListOfRows(list: mutable.ListBuffer[Any]) = {
    list.map(value => toRow(value.asInstanceOf[ScalaAvroRecord].values))
  }
  
  private def convertToMapOfRows(map: mutable.HashMap[Any,Any]) = {
    map.mapValues(value => toRow(value.asInstanceOf[ScalaAvroRecord].values))
  }
  
  private def isListOfRecords(value: mutable.ListBuffer[Any]) = {    
    value.nonEmpty && value.head.isInstanceOf[ScalaAvroRecord]
  }
  
  private def isMapOfRecords(value: mutable.HashMap[Any,Any]) = {
    value.nonEmpty && value.head._2.isInstanceOf[ScalaAvroRecord]
  }
  
  override def get(key: String): Object = {
    val field: Field = schema.getField(key)
    if (field != null) {
      values(field.pos())
    }    
    else {
      field
    }
  }

  override def get(i: Int): Object = {
    values(i)
  }

  override def equals(o: Any): Boolean = {
    if (o == this) {
      return true
    }
    if (!o.isInstanceOf[ScalaAvroRecord]) {
      return false // not a record
    }    
    if (this.schema == null) {
      return false
    }
    
    val that: ScalaAvroRecord = o.asInstanceOf[ScalaAvroRecord]
    
    if (that == null || that.getSchema() == null) {
      return false
    }
    
    if (!this.schema.equals(that.getSchema())) {
      return false // not the same schema
    }    
    GenericData.get().compare(this, that, schema) == 0
  }

  override def hashCode(): Int = {
    GenericData.get().hashCode(this, schema)
  }

  override def compareTo(that: ScalaAvroRecord): Int = {
    GenericData.get().compare(this, that, schema)
  }

  override def toString(): String = {
    GenericData.get().toString(this)
  }

  def getValues(): Array[Object] = values
}  