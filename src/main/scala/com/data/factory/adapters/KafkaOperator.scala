package com.data.factory.adapters

import com.data.factory.exceptions.QueueException
import com.data.factory.ports.Queue
import org.apache.spark.sql.{DataFrame, SparkSession}

class KafkaOperator extends Queue with Serializable {
  private val stringErrorMessage = "%s cannot be null or empty."
  private val objectErrorMessage: String = "%s cannot be null."
  private var session: SparkSession = _

  def this(session: SparkSession) {
    this()
    if (session == null) throw QueueException(objectErrorMessage.format("session"))
    else this.session = session
  }

  def getData(topicName: String, bootstrapServer: String): DataFrame = {
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

}
