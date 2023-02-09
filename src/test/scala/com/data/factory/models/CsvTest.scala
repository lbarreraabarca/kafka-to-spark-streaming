package com.data.factory.models

import com.data.factory.exceptions.RequestException
import org.json4s.NoTypeHints
import org.json4s.native.Serialization
import org.json4s.native.Serialization.read
import org.scalatest._

class CsvTest extends FlatSpec {

  val path = "/path/file.csv"
  val delimiter = "|"
  val header = true

  "constructor" should "create a valid object when receive valid attributes" in {

    val csv = new Csv(path, delimiter, header)
    assert(csv.isValid())
    assert(path == csv.path)
    assert(delimiter == csv.delimiter)
    assert(header == csv.header)
  }

  it should "create a valid object from json" in {
    val jsonInput = "{\"path\":\"/path/file.csv\", \"delimiter\": \"|\", \"header\": true}"
    implicit val formats = Serialization.formats(NoTypeHints)
    val csv = read[Csv](jsonInput)
    assert(csv.isValid())
    assert(path == csv.path)
    assert(delimiter == csv.delimiter)
    assert(header == csv.header)
  }

  it should "throw RequestException because path is null" in {
    val csv = new Csv(null, delimiter, header)
    val exception = intercept[RequestException] {
      csv.isValid()
    }
  }

}
