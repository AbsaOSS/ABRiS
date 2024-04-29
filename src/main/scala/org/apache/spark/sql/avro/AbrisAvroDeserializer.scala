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

package org.apache.spark.sql.avro

import org.apache.avro.Schema
import org.apache.spark.sql.types.DataType
import za.co.absa.commons.annotation.DeveloperApi

import scala.collection.mutable
import scala.util.Try

/**
 * Compatibility layer handling different versions of AvroDeserializer
 * the package also allows to access package private class
 */
@DeveloperApi
class AbrisAvroDeserializer(rootAvroType: Schema, rootCatalystType: DataType) {

  private val deserializer = {
    val clazz = classOf[AvroDeserializer]
    val schemaClz = classOf[Schema]
    val dataTypeClz = classOf[DataType]
    val stringClz = classOf[String]
    val booleanClz = classOf[Boolean]

    clazz.getConstructors.collectFirst {
      case currCtor if currCtor.getParameterTypes sameElements
        Array(schemaClz, dataTypeClz) =>
        // Spark 2.4
        currCtor.newInstance(rootAvroType, rootCatalystType)
      case currCtor if currCtor.getParameterTypes sameElements
        Array(schemaClz, dataTypeClz, stringClz) =>
        // Spark 3.0 - Spark 3.5.0 (including)
        currCtor.newInstance(rootAvroType, rootCatalystType, "LEGACY")
      case currCtor if currCtor.getParameterTypes sameElements
        Array(schemaClz, dataTypeClz, stringClz, booleanClz) =>
        // Spark 3.5.1 - 3.5.2
        currCtor.newInstance(rootAvroType, rootCatalystType, "LEGACY", false: java.lang.Boolean)
      case currCtor if currCtor.getParameterTypes.toSeq sameElements
        Array(schemaClz, dataTypeClz, stringClz, booleanClz, stringClz) =>
        // Spark 4.0.0-SNAPSHOT+
        currCtor.newInstance(rootAvroType, rootCatalystType, "LEGACY", false: java.lang.Boolean, "")
    } match {
      case Some(value: AvroDeserializer) =>
        value
      case _ =>
        throw new NoSuchMethodException(
          s"""Supported constructors for AvroDeserializer are:
             |${clazz.getConstructors.toSeq.mkString(System.lineSeparator())}""".stripMargin)
    }

  }

  private val ru = scala.reflect.runtime.universe
  private val rm = ru.runtimeMirror(getClass.getClassLoader)
  private val classSymbol = rm.classSymbol(deserializer.getClass)
  private val deserializeMethodSymbol = classSymbol.info.decl(ru.TermName("deserialize")).asMethod
  private val deserializeMethod = rm.reflect(deserializer).reflectMethod(deserializeMethodSymbol)

  def deserialize(data: Any): Any = {
    deserializeMethod(data) match {
      case Some(x) => x // Spark 3.1 +
      case x => x // Spark 3.0 -
    }
  }

}
