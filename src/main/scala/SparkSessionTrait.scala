import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

trait SparkSessionTrait {

  // Set the log level to only print errors
  Logger.getLogger("org").setLevel(Level.ERROR)

  // Use new SparkSession interface in Spark 2.0
  val spark = SparkSession
    .builder
    .appName("GeocodAddress")
    .master("local[*]")
    .config("spark.sql.warehouse.dir", "/home/skydev/Documents/temp") // Necessary to work around a Windows bug in Spark 2.0.0; omit if you're not on Windows.
    .config("spark.cassandra.connection.host", "10.234.28.11,10.234.28.50,10.234.28.13") // Necessary to work around a Windows bug in Spark 2.0.0; omit if you're not on Windows.
    .config("spark.cassandra.auth.username", "cassandra") // Necessary to work around a Windows bug in Spark 2.0.0; omit if you're not on Windows.
    .config("spark.cassandra.auth.password", "cassandra") // Necessary to work around a Windows bug in Spark 2.0.0; omit if you're not on Windows.
    .getOrCreate()


}
