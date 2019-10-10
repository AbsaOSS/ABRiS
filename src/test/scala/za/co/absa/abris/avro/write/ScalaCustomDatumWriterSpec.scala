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

package za.co.absa.abris.avro.write

import java.util.{Arrays, Collection}

import org.scalatest.FlatSpec

import scala.collection.JavaConversions._

class ScalaCustomDatumWriterSpec extends FlatSpec {

  // scalastyle:off magic.number

  private val writer = new ScalaCustomDatumWriter

  behavior of "ScalaCustomDatumWriter"

  it should "retrieve the size of Java Collections" in {
    val list = new java.util.ArrayList(Arrays.asList(1))
    val set = new java.util.HashSet(Arrays.asList(1,2))
    val deque = new java.util.ArrayDeque(Arrays.asList(1,2,3))

    assert(writer.getArraySize(list) == list.size())
    assert(writer.getArraySize(set) == set.size())
    assert(writer.getArraySize(deque) == deque.size())
  }

  it should "retrieve the size os Scala Iterables" in {
    val list = List(1,2,3,4)
    val seq = Seq(1,2,3,4,5)
    val set = Set(1,2,3,4,5,6)
    val map = Map(1 -> 2, 2 -> 3)

    assert(writer.getArraySize(list) == list.size)
    assert(writer.getArraySize(seq) == seq.size)
    assert(writer.getArraySize(set) == set.size)
    assert(writer.getArraySize(map) == map.size)
  }

  it should "retrieve the size of Scala arrays" in {
    val array = Array(new Integer(1), new Integer(2), new Integer(3))
    assert(writer.getArraySize(array) == array.size)
  }

  it should "retrieve elements from Java Collections" in {
    val list = new java.util.ArrayList(Arrays.asList(1))
    val set = new java.util.HashSet(Arrays.asList(1,2))
    val deque = new java.util.ArrayDeque(Arrays.asList(1,2,3))

    val listIt = writer.getArrayElements(list)
    assert(contains(list, listIt))

    val setIt = writer.getArrayElements(set)
    assert(contains(set, setIt))

    val dequeIt = writer.getArrayElements(deque)
    assert(contains(deque, dequeIt))
  }

  it should "retrieve elements from Scala Iterables" in {
    val list = List(1,2,3,4)
    val seq = Seq(1,2,3,4,5)
    val set = Set(1,2,3,4,5,6)

    val listIt = writer.getArrayElements(list)
    assert(contains(list, listIt))

    val setIt = writer.getArrayElements(set)
    assert(contains(set, setIt))

    val seqIt = writer.getArrayElements(seq)
    assert(contains(seq, seqIt))
  }

  it should "retrieve elements from Scala arrays" in {
    val array = Array(new Integer(1), new Integer(2), new Integer(3))

    val arrayIt = writer.getArrayElements(array)
    assert(contains(array, arrayIt))
  }

  private def contains(collection: Collection[_ <: Any], iterator: Iterator[Object]): Boolean = {
    if (iterator.size != collection.size()) return false
    while (iterator.hasNext) {
      if (!collection.contains(iterator.next())) {
        return false
      }
    }
    true
  }

  private def contains(array: Array[_ <: Any], iterator: Iterator[Object]): Boolean = {
    if (iterator.size != array.size) return false
    while (iterator.hasNext) {
      if (!array.contains(iterator.next())) {
        return false
      }
    }
    true
  }

  // scalastyle:on magic.number
}
