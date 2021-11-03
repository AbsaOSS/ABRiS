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

package za.co.absa.abris.examples.data.generation

import org.apache.spark.sql.Row
import za.co.absa.commons.annotation.DeveloperApi

import java.nio.ByteBuffer
import scala.collection.JavaConverters.{asScalaBufferConverter, seqAsJavaListConverter}
import scala.collection.{Map, Seq}
import scala.util.Random

/**
 * This class provides methods to generate example/test data.
 * Not part of the library core.
 */
// scalastyle:off magic.number
@DeveloperApi
object ComplexRecordsGenerator {

  private val random = new Random()

  val usedAvroSchema: String = TestSchemas.NATIVE_COMPLETE_SCHEMA

  def generateUnparsedRows(howMany: Int): List[Row] = {
    val result = new Array[Row](howMany)
    for (i <- 0 until howMany) {
      result(i) = Row.fromSeq(getDataSeq())
    }
    result.toList
  }

  private def getDataSeq(): Seq[Object] = {
    val map = Map[String, Seq[Long]](
      "entry1" -> randomSeqOfLongs(20),
      "entry2" -> randomSeqOfLongs(30))
    Seq(
      ByteBuffer.wrap(randomString(20).getBytes).array(),
      randomString(30),
      new java.lang.Integer(random.nextInt()),
      new java.lang.Long(random.nextLong()),
      new java.lang.Double(random.nextDouble()),
      new java.lang.Float(random.nextFloat()),
      new java.lang.Boolean(random.nextBoolean()),
      randomSeqOfStrings(10, 15),
      map,
      new FixedString(randomString(40)).bytes())
  }

  private def randomListOfLongs(listSize: Int) = {
    val array = new Array[Long](listSize)
    for (i <- 0 until listSize) {
      array(i) = random.nextLong()
    }
    new java.util.ArrayList(array.toList.asJava)
  }

  private def randomSeqOfLongs(listSize: Int) = {
    randomListOfLongs(listSize).asScala
  }

  private def randomListOfStrings(listSize: Int, stringLength: Int) = {
    val array = new Array[String](listSize)
    for (i <- 0 until listSize) {
      array(i) = randomString(stringLength)
    }
    new java.util.ArrayList(array.toList.asJava)
  }

  private def randomSeqOfStrings(listSize: Int, stringLength: Int) = {
    randomListOfStrings(listSize, stringLength).asScala
  }

  private def randomString(length: Int): String = {
    val randomStream: Stream[Char] = Random.alphanumeric
    randomStream.take(length).mkString
  }
}
// scalastyle:on magic.number
