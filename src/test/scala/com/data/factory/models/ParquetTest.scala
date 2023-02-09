package com.data.factory.models

import org.json4s.NoTypeHints
import org.json4s.native.Serialization
import org.json4s.native.Serialization.read
import org.scalatest._
class ParquetTest extends FlatSpec {

  val path = "/path/file.csv"

  "constructor" should "create a valid object when receive valid attributes" in {
    val parquet = new Parquet(path)
    assert(parquet.isValid())
    assert(path == parquet.path)
  }

  it should "create a valid object from json" in {
    val jsonInput = "{\"path\":\"/path/file.csv\"}"
    implicit val formats = Serialization.formats(NoTypeHints)
    val parquet = read[Parquet](jsonInput)
    assert(parquet.isValid())
    assert(path == parquet.path)
  }
}

