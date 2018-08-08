import java.io.File

import org.apache.avro.Schema
import org.apache.spark.sql.functions._

object Esim extends SparkSessionTrait {
  def main(args: Array[String]): Unit = {

    val cass_conf = Map("spark.cassandra.connection.host"-> "10.233.241.145", "spark.cassandra.auth.username"-> "cassandra"
      ,"spark.cassandra.auth.password"-> "cassandra")

    val tag_state = spark
      .read
      .format("org.apache.spark.sql.cassandra")
      .options(cass_conf)
      .options(Map("table" -> "tag_state", "keyspace" -> "skydl_fnd_esim_dev"))
      .load()
      .withColumn("now", unix_timestamp(current_timestamp()))
      .withColumn("ts_sec", unix_timestamp(col("ts")))
      .withColumn("diff_time", expr("now - ts_sec"))

    tag_state.createOrReplaceTempView("tag_state")

    val esim_ref_slm = spark
      .read
      .format("org.apache.spark.sql.cassandra")
      .options(cass_conf)
      .options(Map("table" -> "esim_ref_slm", "keyspace" -> "skydl_fnd_esim_dev"))
      .load()

    esim_ref_slm.createOrReplaceTempView("esim_ref_slm")


    val schema = new Schema.Parser().parse(new File("Model/com.amdocs.sky.esim.Violation.avsc"))
    val schemaString = schema.toString

    val violation = spark.sql(
      """
        |select md5(concat(tag, current_timestamp())) as violation_id
        |    ,1000 * unix_timestamp(current_timestamp()) as event_time
        |    ,slm_id as metric_id
        |    ,tag as error_code
        |    ,threshold as max_threshold
        |    ,cast(nb_errors as string) as current_threshold
        |    ,duration as duration_interval
        |    ,duration  as alert_interval
        |    ,description
        |from
        |(
        |    select count(*) nb_errors  , slm_id, tag, threshold, duration, description
        |    from tag_state a
        |        join esim_ref_slm b
        |            on a.tag = b.slm_metric
        |                and a.diff_time < b.duration
        |    group by slm_id,  tag, threshold, duration, description
        |) A
        |where nb_errors < threshold    ---     TO CHANGE : "<" --> ">"
      """.stripMargin)
      .withColumn("value", struct("*"))
      .withColumn("key", col("violation_id"))
      .drop(col("tag"))

//    violation.printSchema()

    violation.select("key", "value").show(1, false)

    val violation2 =  violation.select("key","value")
            .withColumn("avro_value", AvroSer.encodeUDF(schemaString)(col("value")))
            .selectExpr("key", "avro_value as value")

    violation2.show(1, false)

    val botstrap_servers = "10.233.241.143:9600"
    val topic = "sky_fnd_esim_dev_EsimSysAdmin"

    violation2.write
      .format("kafka")
      .option("kafka.bootstrap.servers", botstrap_servers)
      .option("topic", topic)
      .save()

  }
}
