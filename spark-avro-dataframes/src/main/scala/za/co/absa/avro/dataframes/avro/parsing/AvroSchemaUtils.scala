package za.co.absa.avro.dataframes.avro.parsing

import org.apache.avro.Schema
import org.apache.hadoop.fs.FileSystem
import org.apache.commons.io.IOUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import scala.collection.JavaConverters.asScalaBufferConverter

object AvroSchemaUtils {
  
  def parse(schema: String): Schema = new Schema.Parser().parse(schema)  
  
  def load(path: String) = {    
    parse(loadFromHdfs(path))
  }    
  
  def loadPlain(path: String) = {
    loadFromHdfs(path)
  }
  
  private def loadFromHdfs(path: String): String = {
    val hdfs = FileSystem.get(new Configuration())
    val stream = hdfs.open(new Path(path))
    try IOUtils.readLines(stream).asScala.mkString("\n") finally stream.close()
  }  
}