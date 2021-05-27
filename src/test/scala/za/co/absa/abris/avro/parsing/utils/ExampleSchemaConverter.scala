package za.co.absa.abris.avro.parsing.utils

import scala.collection.JavaConverters._
import scala.util.Random

import org.apache.avro.{LogicalTypes, Schema, SchemaBuilder}
import org.apache.avro.LogicalTypes.{Date, Decimal, TimestampMicros, TimestampMillis}
import org.apache.avro.Schema.Type._

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.sql.catalyst.util.RandomUUIDGenerator
import org.apache.spark.sql.types._
import org.apache.spark.sql.types.Decimal.minBytesForPrecision

/**
 * This is an example of a way to build a schema that can compile with avro tools
 */
case class ExampleSchemaConverter() extends AvroSchemaConverter {

  private lazy val nullSchema = Schema.create(Schema.Type.NULL)

  override def toAvroType(catalystType: DataType, nullable: Boolean, recordName: String, nameSpace: String): Schema = {
    val builder = SchemaBuilder.builder()

    val schema = catalystType match {
      case BooleanType => builder.booleanType()
      case ByteType | ShortType | IntegerType => builder.intType()
      case LongType => builder.longType()
      case DateType =>
        LogicalTypes.date().addToSchema(builder.intType())
      case TimestampType =>
        LogicalTypes.timestampMicros().addToSchema(builder.longType())

      case FloatType => builder.floatType()
      case DoubleType => builder.doubleType()
      case StringType => builder.stringType()
      case NullType => builder.nullType()
      case d: DecimalType =>
        val avroType = LogicalTypes.decimal(d.precision, d.scale)
        val fixedSize = minBytesForPrecision(d.precision)
        // Need to avoid naming conflict for the fixed fields
        val name = nameSpace match {
          case "" => s"$recordName.fixed"
          case _ => s"$nameSpace.$recordName.fixed"
        }
        avroType.addToSchema(SchemaBuilder.fixed(name).size(fixedSize))

      case BinaryType => builder.bytesType()
      case ArrayType(et, containsNull) =>
        builder.array()
          .items(toAvroType(et, containsNull, recordName, nameSpace))
      case MapType(StringType, vt, valueContainsNull) =>
        builder.map()
          .values(toAvroType(vt, valueContainsNull, recordName, nameSpace))
      case st: StructType =>
        val childNameSpace = if (nameSpace != "") s"$nameSpace.${recordName.capitalize}nmsp" else recordName
        val fieldsAssembler = builder.record(recordName.capitalize).namespace(nameSpace).fields()
        st.foreach { f =>
          val fieldAvroType =
            toAvroType(f.dataType, f.nullable, f.name, childNameSpace)
          fieldsAssembler.name(f.name).`type`(fieldAvroType).noDefault()
        }
        fieldsAssembler.endRecord()

      // This should never happen.
      case other => throw new RuntimeException(s"Unexpected type $other.")
    }
    if (nullable && catalystType != NullType) {
      Schema.createUnion(schema, nullSchema)
    } else {
      schema
    }
  }
}
