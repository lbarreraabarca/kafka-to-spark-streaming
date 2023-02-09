package com.data.factory.models

import org.json4s.NoTypeHints
import org.json4s.native.Serialization
import org.json4s.native.Serialization.read
import org.scalatest._
class AvroTest extends FlatSpec {

  val path = "/path/file.avro"

  "constructor" should "create a valid object when receive valid attributes" in {
    val avro = new Avro(path)
    assert(avro.isValid())
    assert(path == avro.path)
  }

  it should "create a valid object from json" in {
    val jsonInput = "{\"path\":\"/path/file.avro\"}"
    implicit val formats = Serialization.formats(NoTypeHints)
    val avro = read[Avro](jsonInput)
    assert(avro.isValid())
    assert(path == avro.path)
  }
}
