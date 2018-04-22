package com.keepcoding.batchlayer

import com.databricks.spark.avro._
import com.keepcoding.dominio.{cliente, transaccion}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.util.LongAccumulator

object metricasSparkSQL {

  def run(args: Array[String]): Unit={

    val sparkSession = SparkSession.builder().master("local[*]").appName("Practica final - Batch Layer - Spark SQL")
      .getOrCreate()

    import sparkSession.implicits._

    val rddTransacciones = sparkSession.read.csv(s"file:///${args{0}}")

    val cabecera = rddTransacciones.first

    val rddSinCabecera = rddTransacciones.filter(!_.equals(cabecera)).map(_.toString().split(","))

    val acumuladorT: LongAccumulator = sparkSession.sparkContext.longAccumulator("dniTransaccion")

    //transaccion(DNI:Long, nombre:String, importe:String, fecha:String,descripcion:String, categoria:String, tarjetaCredito:String)

    //cliente(ciudad:String, nombre:String, cuentaCorriente:String)

    val dfClientes= rddSinCabecera.map(columna => {
      cliente(columna(5), columna(4), columna(6))
    })
    //dfClientes.show(10)

    def categoria_ocio(n: String): String = if ( n=="Sports"|n=="Shopping Mall"|n=="Cinema"|n=="Restaurant") "ocio" else "n/a"
    def elim_ultimo_caracter(n:String): String = n.substring(0,n.length-1)

    val dfTransaccion_ = rddSinCabecera.map(columna => {
      acumuladorT.add(acumuladorT.value + 1)
      transaccion(acumuladorT.value, columna(4), columna(2), columna(0).substring(1,columna(0).length),
        elim_ultimo_caracter(columna(10)), categoria_ocio(elim_ultimo_caracter(columna(10))), columna(3))
    })
    val dfTransaccion = dfTransaccion_.withColumn("fecha", to_date($"fecha","MM/dd/yy"))
    //dfTransaccion.show(10)

    dfTransaccion.createOrReplaceGlobalTempView("TRANSACCIONES")
    dfClientes.createOrReplaceGlobalTempView("CLIENTES")

    val joinByName = sparkSession.sql("SELECT p.nombre, p.ciudad FROM global_temp.CLIENTES p, global_temp.TRANSACCIONES d WHERE p.nombre=d.nombre")
      .createOrReplaceGlobalTempView("JBN")
    val countByCity = sparkSession.sql("SELECT ciudad, COUNT(*) AS numTransacciones FROM global_temp.JBN GROUP BY ciudad")
    countByCity.write.mode("append").avro(args(1))

    val dfOutput2= sparkSession.sql("SELECT nombre, importe FROM global_temp.TRANSACCIONES WHERE importe >= 5000")
    dfOutput2.write.mode("append").avro(args(1))

    val dfOutput3 = sparkSession.sql("SELECT a.nombre, COUNT(a.nombre) AS transacciones FROM global_temp.CLIENTES a WHERE a.ciudad = '${args{2}}' GROUP BY a.nombre")
    dfOutput3.write.mode("append").avro(args(1))

    val dfOutput4 = sparkSession.sql("SELECT * FROM global_temp.TRANSACCIONES WHERE categoria='ocio'")
    dfOutput4.write.mode("append").avro(args(1))

    val dfOutput5 = sparkSession.sql("SELECT nombre, COUNT(nombre) AS transacciones FROM global_temp.TRANSACCIONES WHERE fecha >= '1-1-2009' GROUP BY nombre")
    dfOutput5.write.mode("append").avro(args(1))

  }

}
