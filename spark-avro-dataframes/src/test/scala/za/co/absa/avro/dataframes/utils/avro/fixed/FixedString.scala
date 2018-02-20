package za.co.absa.avro.dataframes.utils.avro.fixed

import org.apache.avro.generic.GenericFixed

object FixedString {
  def getClassName() = new FixedString("").getClass.getName
}

class FixedString(value: String) extends GenericFixed {
  override def getSchema() = null
  override def bytes() = value.getBytes
}