/*
 * Copyright 2019 ABSA Group Limited
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

package za.co.absa.abris.avro.sql

import org.apache.avro.{Schema, SchemaBuilder}
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.avro.SchemaConverters.toAvroType

/**
 * This class encapsulate logic and data necessary to get an Avro Schema from various sources
 *
 * Abris library doesn't support serialization of primitive types, therefore if there is only one primitive type
 * it must be internally wrapped in record before it is serialized and the schema must be changed to match.
 *
 * @param schemaGenerator function that generates schema
 */
class SchemaProvider private(private val schemaGenerator: Expression => (Schema, Schema))
  extends Serializable {

  @transient private var cachedUnwrappedSchema: Schema = _
  @transient private var cachedWrappedSchema: Schema = _

  private def lazyLoadSchemas(expression: Expression): Unit = {
    if (cachedUnwrappedSchema == null) {
      val schemas = schemaGenerator(expression)
      cachedUnwrappedSchema = schemas._1
      cachedWrappedSchema = schemas._2
    }
  }

  /**
   *
   * @param expression catalyst expression
   * @return unwrapped schema (it can be just one type without any record)
   */
  def unwrappedSchema(expression: Expression): Schema = {
    lazyLoadSchemas(expression)
    cachedUnwrappedSchema
  }

  /**
   *
   * @param expression catalyst expression
   * @return wrapped schema (always record, even if it contains only one primitive type)
   */
  def wrappedSchema(expression: Expression): Schema = {
    lazyLoadSchemas(expression)
    cachedWrappedSchema
  }
}

object SchemaProvider {

  def apply(schemaString: String): SchemaProvider = {

    new SchemaProvider((_: Expression) => {
      val schema = new Schema.Parser().parse(schemaString)

      def unwrapSparkSchemaIfNeeded(schema: Schema) = {
        if (schema.getFields.size() == 1) {
          schema.getFields.get(0).schema()
        } else {
          schema
        }
      }

      (unwrapSparkSchemaIfNeeded(schema), schema)
    })
  }

  def apply(): SchemaProvider = {
    apply("default", "default")
  }

  def apply(name: String, namespace: String): SchemaProvider = {

    new SchemaProvider((expression: Expression) => {

      val schema = toAvroType(expression.dataType, expression.nullable, name, namespace)

      (schema, wrapSchema(schema, name, namespace))
    })
  }

  private def wrapSchema(schema: Schema, name: String, namespace: String) = {
    if (schema.getType == Schema.Type.RECORD) {
      schema
    } else {
      SchemaBuilder.record(name)
        .namespace(namespace)
        .fields().name(schema.getName).`type`(schema).noDefault()
        .endRecord()
    }
  }
}
