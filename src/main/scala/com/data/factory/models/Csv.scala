package com.data.factory.models

import com.data.factory.exceptions.RequestException
import com.data.factory.utils.FieldValidator
class Csv extends Serializable{

  var path: String = _
  var delimiter: String = _
  var header: Boolean = _
  def this(path: String, delimiter: String, header: Boolean) = {
    this()
    this.path = path
    this.delimiter = delimiter
    this.header = header
  }

  def isValid(): Boolean = try {
    val validator = new FieldValidator()
    validator.validStringField("path")(path)
    validator.validStringField("delimiter")(delimiter)
    validator.validBoolean("header")(header)
  } catch  {
    case e: Exception => throw RequestException("%s %s".format(e.getClass, e.getMessage))
  }
}
