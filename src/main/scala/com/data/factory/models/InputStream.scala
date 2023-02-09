package com.data.factory.models

import com.data.factory.exceptions.RequestException

class InputStream extends Serializable {

  var kafka: Kafka = _

  def this(kafka: Kafka){
    this()
    this.kafka = kafka
  }

  def isValid(): Boolean = try {
    kafka.isValid()
  } catch {
    case e: Exception => throw RequestException("%s %s".format(e.getClass, e.getMessage))
  }

}
