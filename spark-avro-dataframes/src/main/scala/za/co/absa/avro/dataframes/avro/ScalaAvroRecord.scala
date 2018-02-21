package za.co.absa.avro.dataframes.avro

import org.apache.avro.AvroRuntimeException
import org.apache.avro.Schema
import org.apache.avro.Schema.Field
import org.apache.avro.Schema.Type
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericRecord
import org.apache.spark.sql.Row
import org.apache.avro.generic.GenericData.Fixed

/**
 * In order to support Spark-compliant queryable nested structures, nested Avro records need to be converted into Spark Rows.
 *
 * This class extends Avro's GenericRow to perform the conversion at read time.
 */
class ScalaAvroRecord(schema: Schema) extends GenericRecord with Comparable[ScalaAvroRecord] {

  private var values: Array[Object] = _

  if (schema == null || !Type.RECORD.equals(schema.getType()))
    throw new AvroRuntimeException("Not a record schema: " + schema)
  values = new Array[Object](schema.getFields().size())

  override def getSchema(): Schema = {
    schema
  }

  override def put(key: String, value: Object): Unit = {
    val field: Schema.Field = schema.getField(key);
    if (field == null) {
      throw new AvroRuntimeException("Not a valid schema field: " + key);
    }
    put(field.pos(), value)
  }

  override def put(position: Int, value: Object): Unit = {
    values(position) = value match {
      case v: ScalaAvroRecord     => toRow(value.asInstanceOf[ScalaAvroRecord].values)
      case v: java.nio.ByteBuffer => v.array()
      case v: Fixed               => v.bytes()
      case default                => default
    }
  }

  private def toRow(values: Array[Object]) = {
    if (values.length == 1) {
      Row(values(0))
    } else {
      Row.fromSeq(values.toSeq)
    }
  }

  override def get(key: String): Object = {
    val field: Field = schema.getField(key)
    if (field != null) {
      values(field.pos())
    }
    field
  }

  override def get(i: Int): Object = {
    values(i)
  }

  override def equals(o: Any): Boolean = {
    if (o == this) {
      true
    }
    if (!(o.isInstanceOf[ScalaAvroRecord])) {
      false // not a record
    }
    val that: ScalaAvroRecord = o.asInstanceOf[ScalaAvroRecord]
    if (!this.schema.equals(that.getSchema()))
      return false; // not the same schema
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