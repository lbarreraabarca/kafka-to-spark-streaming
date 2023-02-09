package com.data.factory.models

import com.data.factory.adapters.Base64Encoder
import com.data.factory.exceptions.RequestException
import org.json4s.NoTypeHints
import org.json4s.native.Serialization
import org.json4s.native.Serialization.read
import org.scalatest._
class PayloadTest extends FlatSpec {

  "constructor" should "create a valid object when receive valid attributes" in {
    val topicName: String = "mytopic"
    val bootStrapServer: String = "localhost:9000"
    val messageStruct: MessageStruct = new MessageStruct("fieldName", "StringType", true)
    val messageStructList: List[MessageStruct] = List(messageStruct)
    val kafka = new Kafka(topicName, bootStrapServer, messageStructList)
    val inputStream: InputStream = new InputStream(kafka)
    val csv = new Csv("path", ",", true)
    val outputStream: OutputStream = new OutputStream(csv)

    val payload = new Payload(inputStream, outputStream)
    assert(payload.isValid)
  }

  it should "create a valid object when receive a valid json." in {
    val jsonEncoded = "ewogICJpbnB1dFN0cmVhbSI6IHsKICAgICJrYWZrYSI6IHsKICAgICAgInRvcGljTmFtZSI6ICJmaXJzdF90b3BpYyIsCiAgICAgICJib290U3RyYXBTZXJ2ZXIiOiAibG9jYWxob3N0OjI5MDkyIiwKICAgICAgIm1lc3NhZ2VTdHJ1Y3QiOiBbCiAgICAgICAgewogICAgICAgICAgImZpZWxkTmFtZSI6ICJmaXJzdE5hbWUiLAogICAgICAgICAgImZpZWxkVHlwZSI6ICJTdHJpbmdUeXBlIiwKICAgICAgICAgICJudWxsYWJsZSI6IHRydWUKICAgICAgICB9LAogICAgICAgIHsKICAgICAgICAgICJmaWVsZE5hbWUiOiAiYWdlIiwKICAgICAgICAgICJmaWVsZFR5cGUiOiAiSW50ZWdlclR5cGUiLAogICAgICAgICAgIm51bGxhYmxlIjogdHJ1ZQogICAgICAgIH0sCiAgICAgICAgewogICAgICAgICAgImZpZWxkTmFtZSI6ICJib3JuRGF0ZSIsCiAgICAgICAgICAiZmllbGRUeXBlIjogIkRhdGVUeXBlIiwKICAgICAgICAgICJudWxsYWJsZSI6IHRydWUKICAgICAgICB9CiAgICAgIF0KICAgIH0KICB9LAogICJvdXRwdXRTdHJlYW0iOiB7CiAgICAiY3N2IjogewogICAgICAicGF0aCI6ICJDOlxcVXNlcnNcXGx1YmFyXFxPbmVEcml2ZVxcRG9jdW1lbnRvc1xcR2l0SHViXFxrYWZrYS10by1zcGFyay1zdHJlYW1pbmciLAogICAgICAiZGVsaW1pdGVyIjogIiwiLAogICAgICAiaGVhZGVyIjogdHJ1ZQogICAgfQogIH0KfQ"
    val jsonDecoded = new Base64Encoder().decode(jsonEncoded)
    implicit val formats = Serialization.formats(NoTypeHints)
    val request = read[Payload](jsonDecoded)
    request.isValid()
  }
  /*
  it should "throw RequestException when receive an empty query." in {
    val path = "/path/file.csv"
    val delimiter = "|"
    val header = true
    val csv = new Csv(path, delimiter, header)
    val parquet = new Parquet(path)
    val csvTableName = "myCsvTable"
    val parquetTableName = "myParquetTable"
    val csvTable = new TableSpec(csvTableName, csv)
    val parquetTable = new TableSpec(parquetTableName, parquet)
    val encodedQuery = ""
    val outputTable: TableSpec = new TableSpec("output_table", csv)
    val payload = new Payload(List(csvTable, parquetTable), encodedQuery, outputTable)
    val exception = intercept[RequestException] {
      payload.isValid
    }
  }

 */
}
