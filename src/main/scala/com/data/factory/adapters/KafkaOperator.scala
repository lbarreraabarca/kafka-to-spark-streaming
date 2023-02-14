package com.data.factory.adapters

import com.data.factory.exceptions.QueueException
import com.data.factory.models.{MessageStruct, OutputStream}
import com.data.factory.ports.Queue
import com.typesafe.scalalogging.Logger
import org.apache.spark.sql.functions.{col, from_json}
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types._

class KafkaOperator extends Queue with Serializable {
  private val log = Logger("KafkaOperator")
  private val stringErrorMessage = "%s cannot be null or empty."
  private val objectErrorMessage: String = "%s cannot be null."
  private var session: SparkSession = _

  def this(session: SparkSession) {
    this()
    if (session == null) throw QueueException(objectErrorMessage.format("session"))
    else this.session = session
  }

  override def getData(topicName: String, bootstrapServer: String): DataFrame = {
    if (!Option(topicName).isDefined || topicName.isEmpty) throw QueueException(stringErrorMessage.format("topicName"))
    if (!Option(bootstrapServer).isDefined || bootstrapServer.isEmpty) throw QueueException(stringErrorMessage.format("kafkaEndpoint"))
    session
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", bootstrapServer)
      .option("subscribe", topicName)
      .option("includeHeaders", "true")
      //.option("failOnDataLoss", "false")
      .load()
  }

  override def parseData(df: DataFrame, fields: List[MessageStruct]): DataFrame = try {
    log.info("Extracting fields.")
    val columnsNames = fields.map(f => col("parsed.%s".format(f.fieldName)))
    df.selectExpr("cast(value as string) as payload")
      .select(from_json(col("payload").cast("string"), getSchema(fields)).as("parsed"))
      .select(columnsNames: _*)
  } catch {
    case e: Exception => throw QueueException("%s %s".format(e.getClass, e.getMessage))
  }

  def getSchema(fields: List[MessageStruct]): StructType = {
    log.info("Adding fields")
    log.info("Schema generated successfully.")
    var fieldList = List[StructField]()
    for (f <- fields) fieldList = fieldList :+ StructField(f.fieldName, getDataType(f.fieldType), f.nullable)
    log.info("fieldList size : %s".format(fieldList.size) )
    StructType(fieldList)
  }

  private val getDataType: String => org.apache.spark.sql.types.DataType = fieldType => {
    fieldType match {
      case "BooleanType" => org.apache.spark.sql.types.BooleanType
      case "BinaryType" => org.apache.spark.sql.types.BinaryType
      case "ByteType" => org.apache.spark.sql.types.ByteType
      case "DateType" => org.apache.spark.sql.types.DateType
      case "DoubleType" => org.apache.spark.sql.types.DoubleType
      case "FloatType" => org.apache.spark.sql.types.FloatType
      case "IntegerType" => org.apache.spark.sql.types.IntegerType
      case "LongType" => org.apache.spark.sql.types.LongType
      case "StringType" => org.apache.spark.sql.types.StringType
      case "TimestampType" => org.apache.spark.sql.types.TimestampType
      case _ => throw QueueException("unimplemented %s DataType.".format(fieldType))
    }
  }

  override def writeData(df: DataFrame, outputStream: OutputStream): Unit = {
    if(!Option(df).isDefined) throw QueueException("df cannot be null.")
    if(!Option(outputStream).isDefined) throw QueueException("outputStream cannot be null")
    val checkPointPath = scala.util.Properties.envOrElse("CHECKPOINT_PATH", "undefined")
    if (!Option(checkPointPath).isDefined || checkPointPath.isEmpty) throw QueueException("CHECKPOINT_PATH cannot be null.")
    outputStream.tableType() match {
      case "csv" => df.writeStream
        .format("csv")
        .trigger(Trigger.ProcessingTime("10 seconds"))
        .option("checkpointLocation", checkPointPath)
        .option("path", outputStream.csv.path)
        .outputMode("append")
        .start()
        .awaitTermination()
      case "parquet" => df.writeStream
        .format("parquet")
        .option("checkpointLocation", "%s/checkpoint/".format(outputStream.csv.path))
        .option("path", "%s/data".format(outputStream.csv.path))
        .start()
        .awaitTermination()
      case _ => throw QueueException("unimplemented %s format.".format(outputStream.tableType()))
    }
  }
}
