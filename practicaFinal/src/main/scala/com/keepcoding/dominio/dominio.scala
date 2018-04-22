package com.keepcoding.dominio

//case class geolocalizacion(latitud:Double, longitud:Double, ciudad:String, pais:String)

case class transaccion(DNI:Long, nombre:String, importe:String, fecha:String,descripcion:String, categoria:String, tarjetaCredito:String)

case class cliente(ciudad:String, nombre:String, cuentaCorriente:String)

case class tabla (fecha:String, importe:Float, tipo:String, nombre:String, ciudad:String, cC:String, categoria:String)


