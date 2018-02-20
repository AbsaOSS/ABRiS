package za.co.absa.avro.dataframes.utils.avro.fixed

import org.apache.avro.generic.GenericFixed
import java.nio.ByteBuffer
import scala.math.BigInt
import java.math.BigInteger

class FixedLong(value: Long) extends GenericFixed {
  override def getSchema() = null
  override def bytes() = BigInteger.valueOf(value).toByteArray()
}