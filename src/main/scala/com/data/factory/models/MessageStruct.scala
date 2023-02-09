package com.data.factory.models

import com.data.factory.exceptions.RequestException
import com.data.factory.utils.FieldValidator

class MessageStruct extends Serializable {

  var fieldName: String = _
  var fieldType: String = _
  var nullable: Boolean = _

  def this(fieldName: String, fieldType: String, nullable: Boolean){
    this()
    this.fieldName = fieldName
    this.fieldType = fieldType
    this.nullable = nullable
  }

  def isValid(): Boolean = try {
    val validator = new FieldValidator()
    validator.validStringField("fieldName")(fieldName)
    validator.validStringField("fieldType")(fieldType)
    validator.validBoolean("nullable")(nullable)
  } catch {
    case e: Exception => throw RequestException("%s %s".format(e.getClass, e.getMessage))
  }
}
