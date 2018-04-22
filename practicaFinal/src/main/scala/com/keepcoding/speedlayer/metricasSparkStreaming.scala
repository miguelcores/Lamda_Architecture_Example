package com.keepcoding.speedlayer

import java.util.Properties

import com.keepcoding.dominio.tabla
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent

object metricasSparkStreaming {

  def run(args: Array[String]): Unit={

    val conf = new SparkConf().setMaster("local[*]").setAppName("Practica FInal - Speed Layer")

    val kafkaParams = Map[String, Object](
      elems = "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "spark-demo",
      "kafka.comsumer.id" -> "kafka-consumer-01"
    )

    val ssc = new StreamingContext(conf, Seconds(1))

    val sparkSession = SparkSession.builder().master("local[*]").appName("Practica final - Batch Layer - Spark SQL")
      .getOrCreate()

    import sparkSession.implicits._

    val inputEnBruto: InputDStream[ConsumerRecord[String,String]] = KafkaUtils.createDirectStream(ssc,
      PreferConsistent, Subscribe[String, String](Array(args(0)), kafkaParams))

    val transaccionesStream: DStream[Array[String]] = inputEnBruto.map(_.value().split(","))

    transaccionesStream.foreachRDD { rdd =>
      def categoria_ocio(n: String): String = if ( n=="Sports"|n=="Shopping Mall"|n=="Cinema"|n=="Restaurant") "ocio" else "n/a"
      def elim_ultimo_caracter(n:String): String = n.substring(0,n.length-1)

      //case class tabla (fecha:String, importe:String, tipo:String, nombre:String, ciudad:String, cC:String, categoria:String)
      val tablon = rdd.map(event => {
        tabla(event(0).toString.substring(1,event(0).toString.length), event(2).toFloat,event(3).toString,
          event(4).toString,event(5).toString, event(6).toString, categoria_ocio(elim_ultimo_caracter(event(10).toString)))
      }).toDF()

      val output1 = tablon.groupBy('ciudad).count()
      val output2 = tablon.select("nombre","importe").where("importe >=5000")
      val output3 = tablon.select("nombre", "tipo", "importe", "cC").where(s"ciudad == '${args{5}}'")
      val output4 = tablon.select("nombre", "tipo", "importe", "cC").where("categoria == 'ocio'")

      val mediaTarjetas = tablon.groupBy('tipo).mean("importe").toDF()
      val transaccionesFecha = tablon.groupBy('fecha).count()

      output1.rdd.foreachPartition(writeToKafka(outputTopic1 = args(1)))
      output2.rdd.foreachPartition(writeToKafka(outputTopic1 = args(2)))
      output3.rdd.foreachPartition(writeToKafka(outputTopic1 = args(3)))
      output4.rdd.foreachPartition(writeToKafka(outputTopic1 = args(4)))

      mediaTarjetas.write.mode("append").csv(args(6))
      transaccionesFecha.write.mode("append").csv(args(7))

       def writeToKafka(outputTopic1: String)(partitionOfRecords: Iterator[Row]): Unit = {
         val producer = new KafkaProducer[String, String](getKafkaConfig())
         partitionOfRecords.foreach(data => producer.send(new ProducerRecord[String, String](outputTopic1, data.toString)))
         producer.flush()
         producer.close()
       }

    }

       ssc.start()
       ssc.awaitTermination()
  }

  def getKafkaConfig(): Properties = {
    val prop = new Properties()
    prop.put("bootstrap.servers", "localhost:9092")
    prop.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    prop.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    prop
  }

}
