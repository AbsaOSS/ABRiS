package za.co.absa.avro.dataframes.examples.data.generation

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