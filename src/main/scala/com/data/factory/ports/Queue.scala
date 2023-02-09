package com.data.factory.ports

import org.apache.spark.sql.DataFrame

trait Queue {
  def getData(topicName: String, kafkaEndpoint: String): DataFrame
}
