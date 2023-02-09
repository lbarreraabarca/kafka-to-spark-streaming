package com.data.factory.models

import com.data.factory.exceptions.RequestException
import com.data.factory.utils.FieldValidator

class Kafka extends Serializable {

  var topicName: String = _
  var bootStrapServer: String = _
  var messageStruct: List[MessageStruct] = _

  def this(topicName: String, bootStrapServer: String, messageStruct: List[MessageStruct]) = {
    this()
    this.topicName = topicName
    this.bootStrapServer = bootStrapServer
    this.messageStruct = messageStruct
  }

  def isValid(): Boolean = try {
    val validator = new FieldValidator()
    messageStruct.map(e => e.isValid())
    validator.validStringField("topicName")(topicName)
    validator.validStringField("bootStrapServer")(bootStrapServer)
  } catch {
    case e: Exception => throw RequestException("%s %s".format(e.getClass, e.getMessage))
  }
}
