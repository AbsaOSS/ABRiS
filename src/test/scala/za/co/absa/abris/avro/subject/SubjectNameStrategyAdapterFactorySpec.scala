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
import org.scalatest.FlatSpec
import za.co.absa.abris.avro.read.confluent.SchemaManager.SchemaStorageNamingStrategies._

class SubjectNameStrategyAdapterFactorySpec extends FlatSpec {

  behavior of SubjectNameStrategyAdapterFactory.getClass.getName

  it should "instantiate TopicNameStrategy" in {
    assert(SubjectNameStrategyAdapterFactory.build(TOPIC_NAME).isAdapteeType(classOf[TopicNameStrategy]))
  }

  it should "instantiate RecordNameStrategy" in {
   assert(SubjectNameStrategyAdapterFactory.build(RECORD_NAME).isAdapteeType(classOf[RecordNameStrategy]))
  }

  it should "instantiate TopicRecordNameStrategy" in {
    assert(SubjectNameStrategyAdapterFactory.build(TOPIC_RECORD_NAME).isAdapteeType(classOf[TopicRecordNameStrategy]))
  }

  it should "throw on invalid strategy" in {
    val message = intercept[IllegalArgumentException] {
      SubjectNameStrategyAdapterFactory.build("any")
    }
    assert(message.getMessage.contains("Invalid strategy"))
  }

  it should "throw on null strategy" in {
    val message = intercept[IllegalArgumentException] {
      SubjectNameStrategyAdapterFactory.build(null)
    }
    assert(message.getMessage.contains("Invalid strategy"))
  }
}
