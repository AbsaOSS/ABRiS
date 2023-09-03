package za.co.absa.abris.avro.endtoend

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.functions.{col, struct}
import org.scalatest.FlatSpec
import za.co.absa.abris.avro.functions.to_avro
import za.co.absa.abris.avro.parsing.utils.AvroSchemaUtils
import za.co.absa.abris.config.AbrisConfig
import za.co.absa.abris.examples.ConfluentKafkaAvroWriter.kafkaTopicName

import java.net.Socket
import java.nio.charset.StandardCharsets

case class SimulatedKakfaRow(key: Array[Byte], value: Array[Byte])
class FullSpec extends FlatSpec {

  it should "work end to end" in {
    assume(hostPortInUse("localhost", 8081))
    val spark = SparkSession.builder().appName("end2end").master("local[1]").getOrCreate()
    implicit val ctx = spark.sqlContext
    import spark.implicits._
    val events = MemoryStream[SimulatedKakfaRow]
    events.addData(SimulatedKakfaRow("key".getBytes(StandardCharsets.UTF_8),
    """{"a":"b", "c", 1} """.getBytes(StandardCharsets.UTF_8)))

    val dataFrame = events.toDF()
    val allColumns = struct(dataFrame.columns.head, dataFrame.columns.tail: _*)

    val schema = AvroSchemaUtils.toAvroSchema(dataFrame)
    val abrisConfig = AbrisConfig
      .toConfluentAvro
      .provideAndRegisterSchema(schema.toString)
      .usingTopicNameStrategy(kafkaTopicName)
      .usingSchemaRegistry("http://localhost:8081")

    val avroFrame = dataFrame.select(to_avro(allColumns, abrisConfig) as 'value)

    avroFrame
      .write
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("topic", kafkaTopicName)
      .save()


    val readConfig = AbrisConfig
      .fromConfluentAvro
      .downloadReaderSchemaByLatestVersion
      .andTopicNameStrategy(kafkaTopicName)
      .usingSchemaRegistry("http://localhost:8081")

    import za.co.absa.abris.avro.functions.from_avro
    val deserialized = dataFrame.select(from_avro(col("value"), readConfig) as 'data)

    deserialized.printSchema()

    deserialized
      .writeStream
      .format("console")
      .option("truncate", "false")
      .start()
      .awaitTermination(5000)


  }

  def hostPortInUse(host: String, port: Int): Boolean = {
    scala.util.Try[Boolean] {
      new Socket(host, port).close()
      true
    }.getOrElse(false)
  }
}
