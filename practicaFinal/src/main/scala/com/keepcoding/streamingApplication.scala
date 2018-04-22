package com.keepcoding

import com.keepcoding.speedlayer.metricasSparkStreaming

object streamingApplication {

  def main(args: Array[String]): Unit = {

    if(args.length == 8){
      metricasSparkStreaming.run(args)
    }else{
      println("Se está intentando arrancar el job de Spark sin los parametros correctos")
    }

  }

}
