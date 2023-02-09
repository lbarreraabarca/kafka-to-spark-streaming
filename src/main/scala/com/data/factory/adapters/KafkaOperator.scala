package com.data.factory.adapters

import com.data.factory.exceptions.QueueException
import com.data.factory.models.{MessageStruct, OutputStream}
import com.data.factory.ports.Queue
import com.typesafe.scalalogging.Logger
import org.apache.spark.sql.functions.{col, from_json}
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
      .load()
  }

  override def parseData(df: DataFrame, fields: List[MessageStruct]): DataFrame = try {
    log.info("Extracting fields.")

    val parsedDf = df.select(from_json(col("value").cast("string"), getSchema(fields)).alias("parsed"))
    //log.info("DataFrame schema %s.".format(parsedDf.printSchema))
    log.info("Selecting columns")
    parsedDf.select(col("parsed.*"))
  } catch {
    case e: Exception => throw QueueException("%s %s".format(e.getClass, e.getMessage))
  }

  def getSchema(fields: List[MessageStruct]): StructType = {
    val schema = new StructType()
    log.info("Adding fields")
    fields.map(f => schema.add(StructField(f.fieldName, getDataType(f.fieldType), f.nullable)))
    log.info("Schema generated successfully.")
    schema
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
    outputStream.tableType() match {
      case "csv" => df.writeStream
        .format("csv")
        .option("checkpointLocation", "%s/checkpoint/".format(outputStream.csv.path))
        .option("path", "%s/data".format(outputStream.csv.path))
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
