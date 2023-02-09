package com.data.factory.adapters



import com.data.factory.exceptions.RequestException
import com.data.factory.models.MessageStruct
import com.typesafe.scalalogging.Logger
import org.json4s.NoTypeHints
import org.json4s.native.Serialization
import org.json4s.native.Serialization.read
import org.scalatest._


class KafkaOperatorTest extends FlatSpec {
  private val log = Logger("KafkaOperatorTest")

  "getSchema" should "return a valid schema." in {
    val field1 = new MessageStruct("field1", "StringType", true)
    val field2 = new MessageStruct("field2", "IntegerType", true)
    val fields: List[MessageStruct] = List(field1, field2)

    val sparkSession = new SparkSessionFactory()
    val spark = sparkSession.makeLocal()
    val operator = new KafkaOperator(spark)
    val schema = operator.getSchema(fields)
  }

}
