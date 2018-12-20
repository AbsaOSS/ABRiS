/*
 *  Copyright 2018 ABSA Group Limited
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package za.co.absa.abris.avro.subject

import io.confluent.kafka.serializers.subject.{RecordNameStrategy, TopicNameStrategy, TopicRecordNameStrategy}
import io.confluent.kafka.serializers.subject.strategy.SubjectNameStrategy
import org.apache.avro.Schema

private[avro] class SubjectNameStrategyAdapter(strategy: SubjectNameStrategy[Schema]) {

  def subjectName(topic: String, isKey: Boolean, schema: Schema): String = strategy.subjectName(topic, isKey, schema)

  def isAdapteeType(c: Class[_ <: SubjectNameStrategy[Schema]]) : Boolean = strategy.getClass == c

  /**
    * Checks if the Schema definition is compatible with the adaptee strategy
    * If the schema's name is provided, every strategy is acceptable, otherwise, only TopicNameStrategy can be used.
    */
  def validate(schema: Schema): Boolean = isValidSchema(schema) || strategyDoesNotDependOnSchema

  private def isValidSchema(schema: Schema) = !(schema == null || schema.getFullName == null)

  private def strategyDoesNotDependOnSchema = isAdapteeType(classOf[TopicNameStrategy])
}
