package com.data.factory.ports

import com.data.factory.models.{MessageStruct, OutputStream}
import org.apache.spark.sql.DataFrame

trait Queue {
  def getData(topicName: String, kafkaEndpoint: String): DataFrame
  def parseData(df: DataFrame, fields: List[MessageStruct]): DataFrame
  def writeData(df: DataFrame, outputStream: OutputStream): Unit
}
