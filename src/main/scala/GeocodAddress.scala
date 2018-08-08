

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import com.datastax.spark.connector._
import org.apache.spark.sql.cassandra._


object GeocodAddress extends SparkSessionTrait {

  def main(args: Array[String]) {


    //    spark.conf.set("spark.cassandra.connection.host", "10.234.28.11,10.234.28.50,10.234.28.13")
    //    spark.conf.set("spark.cassandra.auth.username", "cassandra")
    //    spark.conf.set("spark.cassandra.auth.password", "cassandra")


    val site_inst_poc = spark
      .read
      .cassandraFormat("site_inst_poc", "spark_poc")
      .load()

    site_inst_poc.createOrReplaceTempView("site_inst_poc")

    val site_inst_poc2 = spark.sql("""
      select cast(SITE_INST_ID as int) as  SITE_INST_ID, ADDRESS, CITY, COUNTY, COUNTRY, LONGITUDE, LATITUDE --, clli
      from site_inst_poc
        where ADDRESS is not null
      """)  //,seed

    println(site_inst_poc2.count())


    site_inst_poc2.createOrReplaceTempView("site_inst_poc2")

    //    println(site_inst_poc2.count())

    val REPORT_122_RECONCILIATION = spark
      .read
      .cassandraFormat("REPORT_122_RECONCILIATION", "spark_poc")
      .load()

    REPORT_122_RECONCILIATION.createOrReplaceTempView("tbl_REPORT_122_RECONCILIATION")

    // val seed = 5
    val withReplacement = false //False in pyspark
    val fraction = 0.05

    val REPORT_122_RECONCILIATION_step1 = spark.sql("""
      select
          coalesce(cast(a.SITE_INST_ID as int), b.SITE_INST_ID) as  SITE_INST_ID
          ,coalesce(a.ADDRESS, b.ADDRESS) as ADDRESS
          ,coalesce(a.CITY, b.CITY) as CITY
          ,coalesce(a.COUNTY, b.COUNTY) as COUNTY
          ,coalesce(a.COUNTRY, b.COUNTRY) as COUNTRY
          ,b.COUNT_TESTS
          ,b.LAST_TEST_DATE
      from (select * from site_inst_poc2 where LONGITUDE is null) a
        left outer join tbl_REPORT_122_RECONCILIATION  b -- (select * from tbl_REPORT_122_RECONCILIATION where LONGITUDE is null)  b
      on a.SITE_INST_ID = b.SITE_INST_ID
      where  (b.LONGITUDE is null and b.ADDRESS is not null) or (b.ADDRESS is null)

  """).sample(withReplacement, fraction).limit(300)

    val t = java.time.LocalDate.now
    val key = "d6080af1b975f9"


    val REPORT_122_RECONCILIATION_step2 = REPORT_122_RECONCILIATION_step1.select("SITE_INST_ID", "ADDRESS", "CITY", "COUNTY", "COUNTRY", "COUNT_TESTS")
      .withColumn("adr" , expr("regexp_replace(concat(ADDRESS,CITY, coalesce(COUNTY, '') , coalesce(COUNTRY, '')), ' ' ,'%20')"))
      .withColumn("geoc", GeocodUtils.searchAdressLocationiqUDF(key)(col("adr")))
      .withColumn("GEOCODAGE_STATUS", expr("geoc.status"))
      .withColumn("LATITUDE",get_json_object(col("geoc.geoc_result"), "$.lat"))
      .withColumn("LONGITUDE",get_json_object(col("geoc.geoc_result"), "$.lon"))
      .withColumn("CORRECTION_SOURCE",expr("case when LONGITUDE is null then null else 'API' end "))
      .withColumn("COUNT_TESTS",expr("coalesce(COUNT_TESTS + 1 , 1)"))
      .withColumn("LAST_TEST_DATE",lit(s"$t"))
      .drop("adr")
      .drop("geoc")

    REPORT_122_RECONCILIATION_step2.show(10, false)

    println("nb rows to ask from API : " + REPORT_122_RECONCILIATION_step2.count())

    REPORT_122_RECONCILIATION_step2.show(5, truncate=false)

    REPORT_122_RECONCILIATION_step2.drop("GEOCODAGE_STATUS").write
      .cassandraFormat("REPORT_122_RECONCILIATION", "spark_poc")
      .mode("append")
      .save()


    // WRITE INTO REPORT_122_RECONCILIATION  all SITE_INST_ID without long/lat in REPORT_122_RECONCILIATION and with in SITE_INST
    spark.sql("""
        select
          coalesce(cast(a.SITE_INST_ID as int), b.SITE_INST_ID) as  SITE_INST_ID
          ,coalesce(a.ADDRESS, b.ADDRESS) as ADDRESS
          ,coalesce(a.CITY, b.CITY) as CITY
          ,coalesce(a.COUNTY, b.COUNTY) as COUNTY
          ,coalesce(a.COUNTRY, b.COUNTRY) as COUNTRY
          ,a.LONGITUDE
          ,a.LATITUDE
          ,'SOURCE' as CORRECTION_SOURCE
          ,b.COUNT_TESTS
          ,b.LAST_TEST_DATE
        from site_inst_poc2 a inner join tbl_REPORT_122_RECONCILIATION b
        on a.SITE_INST_ID = b.SITE_INST_ID
        where a.LONGITUDE is not null and b.LONGITUDE is null
        """).write
      .cassandraFormat("REPORT_122_RECONCILIATION", "spark_poc")
      .mode("append")
      .save()


    //    try{
    //      site_inst_poc_withgeoc.createCassandraTable(
    //        "spark_poc",
    //        "REPORT_122_RECONCILIATION",
    //        partitionKeyColumns = Some(Seq("SITE_INST_ID")) //,
    //        //clusteringKeyColumns = Some(Seq("f0"))
    //      )
    //    } catch {
    //      case e: Exception => println("exception caught: " + e);
    //    }
    //
    //    site_inst_poc_withgeoc.write
    //      .cassandraFormat("REPORT_122_RECONCILIATION", "spark_poc")
    //      .mode("append")
    //      .save()

  }
}
