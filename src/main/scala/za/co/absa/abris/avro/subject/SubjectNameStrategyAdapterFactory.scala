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

import io.confluent.kafka.serializers.subject.{RecordNameStrategy, SubjectNameStrategy, TopicNameStrategy, TopicRecordNameStrategy}
import org.slf4j.LoggerFactory
import za.co.absa.abris.avro.read.confluent.SchemaManager.SchemaStorageNamingStrategies._

private [avro] object SubjectNameStrategyAdapterFactory {

  private val logger = LoggerFactory.getLogger(this.getClass.getCanonicalName)

  def build(strategy: String): SubjectNameStrategyAdapter = {

    logger.info(s"Creating adapter for strategy: $strategy")

    strategy match {
      case TOPIC_NAME        => new SubjectNameStrategyAdapter(new TopicNameStrategy)
      case RECORD_NAME       => new SubjectNameStrategyAdapter(new RecordNameStrategy)
      case TOPIC_RECORD_NAME => new SubjectNameStrategyAdapter(new TopicRecordNameStrategy)
      case _                 => throw new IllegalArgumentException(s"Invalid strategy: $strategy")
    }
  }
}
