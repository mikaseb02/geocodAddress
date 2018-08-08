import java.net.{HttpURLConnection, InetSocketAddress, Proxy}

import org.apache.spark.sql.functions._

import scala.io.Source
import scala.util.Try

object GeocodUtils  extends Serializable {

  def  getUrl(url: String ) : String = {
    val proxy = new Proxy(Proxy.Type.HTTP, new InetSocketAddress("genproxy.amdocs.com", 8080))
    val connection: HttpURLConnection = new java.net.URL(url).openConnection(proxy).asInstanceOf[HttpURLConnection]
    connection.connect()
    Source.fromInputStream(connection.getInputStream).getLines.mkString
    //    scala.io.Source.fromURL(url).mkString
  }

  def searchAdressGoogle(address:String):String = {
    val key = "AIzaSyAv7FJ6ow5ftamFhuce2TTRkZMwIDNWBSg" // "d6080af1b975f9"

    val url = s"https://maps.googleapis.com/maps/api/geocode/json?address=$address&key=$key"
//    println(s"url: $url")

    //   val url = s"https://us1.locationiq.org/v1/search.php?key=$key&q=$adresse&format=json"

    val ad = Try(getUrl(url)).getOrElse(null)

    ad

  }

  def searchAdressLocationiq(adresse:String, key:String):(String,String) = {
    // val key = "d6080af1b975f9"
    //    val url = s"https://maps.googleapis.com/maps/api/geocode/json?address=$adresse&key=$key"

    val url = s"https://us1.locationiq.org/v1/search.php?key=$key&q=$adresse&format=json"

    //   println(url)

    val proxy = new Proxy(Proxy.Type.HTTP, new InetSocketAddress("genproxy.amdocs.com", 8080))
    val connection: HttpURLConnection = new java.net.URL(url).openConnection(proxy).asInstanceOf[HttpURLConnection]
    connection.connect()
    val code = Try{connection.getResponseCode().toString}.getOrElse(null)
//    println(code)

    val ad = Try{
      val result = Source.fromInputStream(connection.getInputStream).getLines.mkString
      val l : List[String]  = ("""\{[^\{]+\}""".r findAllIn result).toList
      l.head
    }.getOrElse(null)
    return(ad, code)
  }

  import org.apache.spark.sql.types.{ StringType, StructType}
  val schema = new StructType()
    .add("geoc_result", StringType)
    .add("status", StringType)


  def searchAdressLocationiqUDF(key:String) = udf((x:String) =>  searchAdressLocationiq(x, key) , schema)
  val searchAdressGoogleUDF = udf(searchAdressGoogle(_:String):String)

}