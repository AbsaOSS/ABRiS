/*
 *
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

package org.apache.spark.sql.avro

import org.apache.avro.Schema
import org.apache.spark.sql.types.DataType

import scala.util.Try

/**
 * Compatibility layer handling different versions of AvroDeserializer
 * the package also allows to access package private class
 */
class AbrisAvroDeserializer(rootAvroType: Schema, rootCatalystType: DataType) {

  private val deserializer = {
    val clazz = classOf[AvroDeserializer]
    Try {
      clazz.getConstructor(classOf[Schema], classOf[DataType])
        .newInstance(rootAvroType, rootCatalystType)
    }.recover { case _: NoSuchMethodException =>
      clazz.getConstructor(classOf[Schema], classOf[DataType], classOf[String])
        .newInstance(rootAvroType, rootCatalystType, "LEGACY")
    }
      .get
      .asInstanceOf[AvroDeserializer]
  }

  private val ru = scala.reflect.runtime.universe
  private val rm = ru.runtimeMirror(getClass.getClassLoader)
  private val classSymbol = rm.classSymbol(deserializer.getClass)
  private val deserializeMethodSymbol = classSymbol.info.decl(ru.TermName("deserialize")).asMethod
  private val deserializeMethod = rm.reflect(deserializer).reflectMethod(deserializeMethodSymbol)

  def deserialize(data: Any): Any = {
    deserializeMethod(data) match {
      case Some(x) => x
      case x => x
    }
  }

}
