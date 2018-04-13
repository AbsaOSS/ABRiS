/*
 * Copyright 2018 Barclays Africa Group Limited
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

package za.co.absa.abris.avro.schemas

import org.apache.avro.Schema
import org.apache.commons.io.IOUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import za.co.absa.abris.avro.read.confluent.SchemaManager

import scala.collection.JavaConverters._

/**
  * This class provides methods for loading Avro schemas from both, file systems and Schema Registry.
  */
object SchemaLoader {

  def loadFromFile(path: String): String = {
    if (path == null) {
      throw new IllegalArgumentException("Null path informed. Please make sure you provide a valid path to an existing " +
        "Avro schema located in some file system.")
    }
    val hdfs = FileSystem.get(new Configuration())
    val stream = hdfs.open(new Path(path))
    try IOUtils.readLines(stream).asScala.mkString("\n") finally stream.close()
  }

  def loadFromSchemaRegistry(params: Map[String,String]): Schema = {
    configureSchemaManager(params)
    val topic = params(SchemaManager.PARAM_SCHEMA_REGISTRY_TOPIC)
    val subject = SchemaManager.getSubjectName(topic, false)
    var paramId = params(SchemaManager.PARAM_SCHEMA_ID)
    val id = getSchemaId(paramId, subject)
    SchemaManager.getBySubjectAndId(subject, id).get
  }

  private def configureSchemaManager(params: Map[String,String]) = {
    if (!SchemaManager.isSchemaRegistryConfigured()) {
      SchemaManager.configureSchemaRegistry(params)
    }
  }

  private def getSchemaId(paramId: String, subject: String): Int = {
    if (paramId == SchemaManager.PARAM_SCHEMA_ID_LATEST_NAME) {
      val latest = SchemaManager.getLatestVersion(subject)
      if (latest.isDefined) {
        latest.get
      }
      else {
        throw new IllegalArgumentException(s"Could not find schema for subject '$subject'. Are you sure the subject exists?")
      }
    }
    else {
      paramId.toInt
    }
  }
}