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

package za.co.absa.abris.avro

import org.apache.spark.sql.DataFrame
import org.scalatest.matchers.should.Matchers._

package object sql {

  /**
   * assert that both dataFrames contain the same data
   *
   * @param expectedFrame
   * @param actualFrame
   */
  def shouldEqualByData(expectedFrame: DataFrame, actualFrame: DataFrame): Unit = {

    def columnNames(frame: DataFrame) = frame.schema.fields.map(_.name)

    val expectedColNames = columnNames(expectedFrame)
    val actualColNames = columnNames(actualFrame)

    expectedColNames shouldEqual actualColNames

    expectedColNames.foreach(col => {
      val expectedColumn = expectedFrame.select(col).collect().map(row => row.toSeq.head)
      val actualColumn = actualFrame.select(col).collect().map(row => row.toSeq.head)

      for ((expected, actual ) <- expectedColumn.zip(actualColumn)) {
        actual shouldEqual expected
      }
    })
  }
}
