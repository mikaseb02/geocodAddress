import java.net.{HttpURLConnection, InetSocketAddress}
import java.net.Proxy.Type

import java.lang.reflect.{Method, InvocationHandler, Proxy}
import GeocodAddress.getUrl

import java.net.InetSocketAddress
import java.net.Proxy


val response: String = try {
  val proxy: Proxy = new Proxy(Proxy.Type.HTTP, new InetSocketAddress("10.0.0.1", 8080))
  val connection: HttpURLConnection = new java.net.URL("myURL").openConnection(proxy).asInstanceOf[HttpURLConnection]
  connection.connect()
  Source.fromInputStream(connection.getInputStream).getLines.mkString
}
catch {
  case e: Throwable => "" // Do whatever you want here: logging/throw exception/..
}




def getUrl(url: String ) : String =
{

  val response: String = try {

    val proxy: Proxy = new Proxy(java.net.Proxy.Type.HTTP, new InetSocketAddress("genproxy.amdocs.com", 8080))
    val connection: HttpURLConnection = new java.net.URL(url).openConnection(proxy).asInstanceOf[HttpURLConnection]
    connection.connect()
    Source.fromInputStream(connection.getInputStream).getLines.mkString
  }
  catch {
    case e: Throwable => "" // Do whatever you want here: logging/throw exception/..
  }

  response
}

def searchAdressGoogle(address:String):String = {
  val key = "AIzaSyAv7FJ6ow5ftamFhuce2TTRkZMwIDNWBSg" // "d6080af1b975f9"

  val url = s"https://maps.googleapis.com/maps/api/geocode/json?address=$address&key=$key"
  println(s"url: $url")

  //   val url = s"https://us1.locationiq.org/v1/search.php?key=$key&q=$adresse&format=json"

  val ad = getUrl(url)

  return ad

}

searchAdressGoogle("10%20rue%20delamare%20deboutteville%2094320%20thiais")



getUrl("")