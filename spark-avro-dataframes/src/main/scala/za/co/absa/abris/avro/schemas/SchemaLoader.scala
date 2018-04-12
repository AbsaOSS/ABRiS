package za.co.absa.abris.avro.schemas

import org.apache.avro.Schema
import org.apache.commons.io.IOUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import za.co.absa.abris.avro.read.confluent.SchemaManager

import scala.collection.JavaConverters._

object SchemaLoader {

  def loadFromFile(path: String): String = {
    val hdfs = FileSystem.get(new Configuration())
    val stream = hdfs.open(new Path(path))
    try IOUtils.readLines(stream).asScala.mkString("\n") finally stream.close()
  }

  def loadFromSchemaRegistry(params: Map[String,String]): Schema = {
    val topic = params(SchemaManager.PARAM_SCHEMA_REGISTRY_TOPIC)
    val id = params(SchemaManager.PARAM_SCHEMA_ID).toInt
    val subject = SchemaManager.getSubjectName(topic, false)
    SchemaManager.getBySubjectAndId(subject, id).get
  }

  def loadPlainFromSchemaRegistry(params: Map[String,String]): String = {
    val topic = params(SchemaManager.PARAM_SCHEMA_REGISTRY_TOPIC)
    val id = params(SchemaManager.PARAM_SCHEMA_ID).toInt
    val subject = SchemaManager.getSubjectName(topic, false)
    SchemaManager.getBySubjectAndId(subject, id).get.toString
  }
}