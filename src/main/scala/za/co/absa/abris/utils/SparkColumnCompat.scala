/*
 * Copyright 2024 ABSA Group Limited
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

package za.co.absa.abris.utils

import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.expressions.Expression

/**
 * Compatibility layer for Column <-> Expression conversions across Spark versions.
 *
 * In Spark 3.x, Column has a direct `.expr` property and a constructor taking an Expression.
 * In Spark 4.x, Column wraps a ColumnNode AST and conversions go through
 * `org.apache.spark.sql.classic.ColumnConversions` and `ExpressionUtils`.
 *
 * This object uses reflection to support both versions from a single codebase,
 * following the same pattern used in AbrisAvroDeserializer.
 */
object SparkColumnCompat {

  /** Convert a Column to a Catalyst Expression. Replaces `column.expr`. */
  lazy val col2expr: Column => Expression = {
    // Try Spark 3.x first: Column.expr is a direct method
    val spark3Method = try {
      Some(classOf[Column].getMethod("expr"))
    } catch {
      case _: NoSuchMethodException => None
    }

    spark3Method match {
      case Some(method) =>
        (column: Column) => method.invoke(column).asInstanceOf[Expression]

      case None =>
        // Spark 4.x: use org.apache.spark.sql.classic.ColumnConversions.expression(column)
        val clazz = Class.forName("org.apache.spark.sql.classic.ColumnConversions$")
        val instance = clazz.getField("MODULE$").get(null)
        val method = clazz.getMethod("expression", classOf[Column])
        (column: Column) => method.invoke(instance, column).asInstanceOf[Expression]
    }
  }

  /** Convert a Catalyst Expression to a Column. Replaces `new Column(expr)`. */
  lazy val expr2col: Expression => Column = {
    // Try Spark 3.x first: new Column(Expression)
    val spark3Ctor = try {
      Some(classOf[Column].getConstructor(classOf[Expression]))
    } catch {
      case _: NoSuchMethodException => None
    }

    spark3Ctor match {
      case Some(ctor) =>
        (expr: Expression) => ctor.newInstance(expr)

      case None =>
        // Spark 4.x: use ExpressionUtils.column(expr)
        val clazz = Class.forName("org.apache.spark.sql.classic.ExpressionUtils$")
        val instance = clazz.getField("MODULE$").get(null)
        val method = clazz.getMethod("column", classOf[Expression])
        (expr: Expression) => method.invoke(instance, expr).asInstanceOf[Column]
    }
  }
}
