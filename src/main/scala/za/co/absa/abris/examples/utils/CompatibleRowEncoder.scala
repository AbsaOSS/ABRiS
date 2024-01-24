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

package za.co.absa.abris.examples.utils

import org.apache.spark.sql.{Encoder, Row}
import org.apache.spark.sql.types.StructType

import scala.util.Try

object CompatibleRowEncoder {
  def apply(schema: StructType): Encoder[Row] = {
    // Spark < 3.5.0
    val rowEncoderTry = Try {
      val rowEncoderClass = Class.forName("org.apache.spark.sql.catalyst.encoders.RowEncoder")
      val applyMethod = rowEncoderClass.getMethod("apply", classOf[StructType])
      applyMethod.invoke(null, schema).asInstanceOf[Encoder[Row]]
    }

    // Spark >= 3.5.0
    rowEncoderTry.orElse(Try {
      val encodersClass = Class.forName("org.apache.spark.sql.Encoders")
      val rowMethod = encodersClass.getMethod("row", classOf[StructType])
      rowMethod.invoke(null, schema).asInstanceOf[Encoder[Row]]
    }).getOrElse {
      throw new IllegalStateException("Neither RowEncoder.apply nor Encoders.row is available in the Spark version.")
    }
  }
}
