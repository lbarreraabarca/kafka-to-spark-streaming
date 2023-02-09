package com.data.factory.models

import com.data.factory.exceptions.RequestException

class Payload extends Serializable{

  var inputStream: InputStream = _
  var outputStream: OutputStream = _

  def this(inputStream: InputStream, outputStream: OutputStream){
    this()
    this.inputStream = inputStream
    this.outputStream = outputStream
  }

  def isValid(): Boolean =  try {
    this.inputStream.isValid()
    this.outputStream.isValid()
  } catch {
    case e: Exception => throw RequestException("%s %s".format(e.getClass, e.getMessage))
  }
}
