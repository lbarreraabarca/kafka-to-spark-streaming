package com.data.factory

import com.data.factory.adapters.{Base64Encoder, KafkaOperator, SparkSessionFactory}
import com.data.factory.exceptions.{ControllerException, RequestException}
import com.data.factory.models.Payload
import com.data.factory.ports.{Encoder, Queue}
import com.typesafe.scalalogging.Logger
import org.json4s.NoTypeHints
import org.json4s.native.Serialization
import org.json4s.native.Serialization.read

object App extends Serializable{
    private val log = Logger("App")

    private def makeRequest(jsonInput: String): Payload = try {
        implicit val formats = Serialization.formats(NoTypeHints)
        read[Payload](jsonInput)
    }catch {
        case e: Exception => throw RequestException(e.getClass.toString.concat(":").concat(e.getMessage.toString))
    }

    def main(args: Array[String]): Unit = {
        val encodedInput = args(0)
        try {
            val checkPointPath = scala.util.Properties.envOrElse("CHECKPOINT_PATH", "undefined")
            if (!Option(checkPointPath).isDefined || checkPointPath.isEmpty) throw RequestException("CHECKPOINT_PATH cannot be null.")
            log.info("Creating SparkSession")
            val sparkSession = new SparkSessionFactory()
            log.info("Creating Spark Cluster")
            val spark = sparkSession.makeLocal()

            val encoder: Encoder = new Base64Encoder()
            log.info("Decoding encodedInput %s".format(encodedInput))
            val decodedInput = encoder.decode(encodedInput)
            log.info("Decoded input %s".format(decodedInput))
            val request: Payload = makeRequest(decodedInput)
            log.info("Request.isValid")
            request.isValid()

            val consumer: Queue = new KafkaOperator(spark)
            log.info("Getting data")
            val df = consumer.getData(request.inputStream.kafka.topicName, request.inputStream.kafka.bootStrapServer)
            log.info("Parsing data")
            val structuredDf = consumer.parseData(df, request.inputStream.kafka.messageStruct)
            log.info("Writing data")
            consumer.writeData(structuredDf, request.outputStream)

            log.info("Stopping and closing spark session.")
            spark.stop()
            spark.close()

            log.info("Process ended successfully.")
        } catch {
            case e: Exception => throw ControllerException(e.getClass.toString.concat(":").concat(e.getMessage))
        }
    }
}
