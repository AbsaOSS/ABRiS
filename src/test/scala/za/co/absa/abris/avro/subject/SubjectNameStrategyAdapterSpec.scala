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
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.types.StructType
import org.scalatest.FlatSpec
import za.co.absa.abris.avro.format.SparkAvroConversions

class SubjectNameStrategyAdapterSpec extends FlatSpec {

  private case class ClassForSchema(a: Int, b: String)
  private val schemaName = "name"
  private val schemaNamespace = "namespace"

  private val topic = "topic_test"

  private val avroSchema = SparkAvroConversions.toAvroSchema(caseClassToSparkSchema, schemaName, schemaNamespace)

  private val topicNameAdapter = new SubjectNameStrategyAdapter(new TopicNameStrategy)
  private val recordNameAdapter = new SubjectNameStrategyAdapter(new RecordNameStrategy)
  private val topicRecordNameAdapter = new SubjectNameStrategyAdapter(new TopicRecordNameStrategy)

  behavior of classOf[SubjectNameStrategyAdapter].getName

  it should "invoke TopicNameStragey for value" in {
    assert(s"$topic-value" == topicNameAdapter.subjectName(topic, false, avroSchema))
  }

  it should "invoke TopicNameStragey for key" in {
    assert(s"$topic-key" == topicNameAdapter.subjectName(topic, true, avroSchema))
  }

  it should "invoke TopicNameStragey for value must not depend on schema" in {
    assert(s"$topic-value" == topicNameAdapter.subjectName(topic, false, null))
  }

  it should "invoke TopicNameStragey for key must not depend on schema" in {
    assert(s"$topic-key" == topicNameAdapter.subjectName(topic, true, null))
  }

  it should "invoke RecordNameStragey for value" in {
    assert(avroSchema.getFullName == recordNameAdapter.subjectName(topic, false, avroSchema))
  }

  it should "invoke RecordNameStragey for key" in {
    assert(avroSchema.getFullName == recordNameAdapter.subjectName(topic, true, avroSchema))
  }

  it should "invoke TopicRecordNameStragey for value" in {
    assert(s"$topic-${avroSchema.getFullName}" == topicRecordNameAdapter.subjectName(
      topic, false, avroSchema))
  }

  it should "invoke TopicRecordNameStragey for key" in {
    assert(s"$topic-${avroSchema.getFullName}" == topicRecordNameAdapter.subjectName(
      topic, true, avroSchema))
  }

  private def caseClassToSparkSchema: StructType = Encoders.product[ClassForSchema].schema
}
