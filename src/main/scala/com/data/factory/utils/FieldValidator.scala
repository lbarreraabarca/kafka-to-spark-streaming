package com.data.factory.utils

import com.data.factory.exceptions.FieldValidatorException

import scala.util.matching.Regex

class FieldValidator {
  private val invalidField: String = "%s cannot be null or empty."

  val validStringField: String => String => Boolean = fieldName => fieldValue =>
    if (!Option(fieldValue).isDefined || fieldValue.isEmpty) throw FieldValidatorException(invalidField.format(fieldName))
    else Option(fieldValue).isDefined || fieldValue.isEmpty

  val validBoolean: String => Boolean => Boolean = fieldName => fieldValue =>
    if (!Option(fieldValue).isDefined) throw FieldValidatorException(invalidField.format(fieldName))
    else Option(fieldValue).isDefined

}
