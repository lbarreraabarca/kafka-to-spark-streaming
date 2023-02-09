package com.data.factory.models

import com.data.factory.exceptions.RequestException
import com.data.factory.utils.FieldValidator

class OutputStream extends Serializable {

  var csv: Csv = _
  var parquet: Parquet = _

  def this(csv: Csv) = {
    this()
    this.csv = csv
  }

  def this( parquet: Parquet) = {
    this()
    this.parquet = parquet
  }

  def isValid(): Boolean = try {
    val validator = new FieldValidator()
    if (Option(this.csv).isDefined) this.csv.isValid()
    else if (Option(this.parquet).isDefined) this.parquet.isValid()
    else false
  } catch {
    case e: Exception => throw RequestException("%s %s".format(e.getClass, e.getMessage))
  }

  def tableType(): String = {
    if (Option(this.csv).isDefined) "csv"
    else if (Option(this.parquet).isDefined) "parquet"
    else "undefined"
  }
}
