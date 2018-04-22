package com.keepcoding

import com.keepcoding.batchlayer.metricasSparkSQL

object batchApplication {

  def main(args: Array[String]): Unit = {

    if(args.length == 3){
      metricasSparkSQL.run(args)
    }else{
      println("Se está intentando arrancar el job de Spark sin los parametros correctos")
    }

  }

}
