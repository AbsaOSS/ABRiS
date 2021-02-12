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

package za.co.absa.abris.avro.parsing.utils

import org.apache.avro.{Schema, SchemaBuilder}
import org.apache.commons.io.IOUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.avro.SchemaConverters.toAvroType
import org.apache.spark.sql.functions.struct

import scala.collection.JavaConverters._

/**
 * This class provides utility methods to cope with Avro schemas.
 */
object AvroSchemaUtils {

  /**
   * Parses a plain Avro schema into an org.apache.avro.Schema implementation.
   */
  def parse(schema: String): Schema = new Schema.Parser().parse(schema)

  /**
   * Loads an Avro org.apache.avro.Schema from the path.
   */
  def load(path: String): Schema = {
    parse(loadPlain(path))
  }

  /**
   * Loads an Avro's plain schema from the path.
   */
  def loadPlain(path: String): String = {
    if (path == null) {
      throw new IllegalArgumentException("Null path informed. " +
        "Please make sure you provide a valid path to an existing Avro schema located in some file system.")
    }
    val hdfs = FileSystem.get(new Configuration())
    val stream = hdfs.open(new Path(path))
    try IOUtils.readLines(stream).asScala.mkString("\n") finally stream.close()
  }

  def toAvroSchema(
    dataFrame: DataFrame,
    columnName: String,
    recordName: String = "topLevelRecord",
    nameSpace: String = ""
  ): Schema = {
    val fieldIndex = dataFrame.schema.fieldIndex(columnName)
    val field = dataFrame.schema.fields(fieldIndex)

    toAvroType(field.dataType, field.nullable, recordName, nameSpace)
  }

  def toAvroSchema(
    dataFrame: DataFrame,
    columnNames: Seq[String]
  ): Schema = toAvroSchema(dataFrame, columnNames, "topLevelRecord", "")

  def toAvroSchema(
    dataFrame: DataFrame,
    columnNames: Seq[String],
    recordName: String,
    nameSpace: String
  ): Schema = {
    val allColumns = struct(columnNames.map(dataFrame.col): _*)
    val expression = allColumns.expr

    toAvroType(expression.dataType, expression.nullable, recordName, nameSpace)
  }

  def toAvroSchema(
    dataFrame: DataFrame
  ): Schema = toAvroSchema(dataFrame, "topLevelRecord", "")

  def toAvroSchema(
    dataFrame: DataFrame,
    recordName: String,
    nameSpace: String
  ): Schema =
    toAvroSchema(dataFrame, dataFrame.columns, recordName, nameSpace)

  def wrapSchema(schema: Schema, name: String, namespace: String): Schema = {
    SchemaBuilder.record(name)
      .namespace(namespace)
      .fields().name(schema.getName).`type`(schema).noDefault()
      .endRecord()
  }

}
