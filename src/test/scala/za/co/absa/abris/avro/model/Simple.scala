package za.co.absa.abris.avro.model

import org.apache.avro.Schema
import org.apache.avro.specific.SpecificRecordBase

class Simple extends SpecificRecordBase {

  override def getSchema: Schema = ???

  override def get(field: Int): AnyRef = ???

  override def put(field: Int, value: Any): Unit = ???
}
