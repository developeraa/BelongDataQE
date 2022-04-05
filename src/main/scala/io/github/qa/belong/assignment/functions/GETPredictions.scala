/*
 * Copyright (c) Belong 2022. Not to be shared publicly.
 */
package io.github.qa.belong.assignment.functions

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, desc, row_number, sum}

class GETPredictions extends Session {

  /**
   * Get Daily preditions from given input variables
   *
   * @return
   * {{{
   *      sensorCounts
   *.join(sensorLocations, "sensor_id")
   *.groupBy("location", "year", "month", "mdate","sensor_description")
   *.agg(sum("hourly_counts").alias("daily_count"))
   *.sort(desc("daily_count")).cache()
   *
   * }}}
   */
  def getDailyPredictions(config: Map[String, String]): DataFrame = {
    val sensorCounts = GetDF(config.getOrElse("sensorCountURL", "https://data.melbourne.vic.gov.au/resource/b2ak-trbp.json"))
    val sensorLocations = GetDF(config.getOrElse("sensorLocationURL", "https://data.melbourne.vic.gov.au/resource/h57g-5234.json"))

    sensorCounts
      .join(sensorLocations, "sensor_id")
      .groupBy("location", "year", "month", "mdate", "sensor_description")
      .agg(sum("hourly_counts").alias("daily_count"))
      .sort(desc("daily_count")).cache()

  }

  /**
   * Monthly rollup view of the predictions which produces per month basis
   *
   * @return dataframe
   * {{{
   *     getDailyPredictions()
   *.groupBy("location", "year", "month")
   *.agg(sum("daily_count").alias("monthly_count"))
   *.sort(desc("monthly_count"))
   *.limit(10)
   * }}}
   */
  def getTopMonthlyPredictions(config: Map[String, String]): DataFrame = {
    getDailyPredictions(config)
      .groupBy("location", "year", "month")
      .agg(sum("daily_count").alias("monthly_count"))
      .withColumn("rank", row_number()
        .over(Window
          .partitionBy("year", "month")
          .orderBy(desc("monthly_count"))))
      .filter(col("rank").leq(10))
      .withColumn("longitude", col("location.longitude"))
      .withColumn("latitude", col("location.latitude"))
      .withColumn("human_address", col("location.human_address"))
      .drop("rank", "location")
  }

  /**
   * Daily rollup view of the predictions which produced per day basis..
   *
   * @return dataframe
   * {{{
   *               getDailyPredictions()
   *.withColumn("rank",row_number()
   *.over(Window
   *.partitionBy ("year","month","mdate")
   *.orderBy(desc("daily_count"))))
   *.filter(col("rank").leq(10))
   *.withColumn("longitude",col("location.longitude"))
   *.withColumn("latitude",col("location.latitude"))
   *.withColumn("human_address",col("location.human_address"))
   *.drop("rank","location")
   * }}}
   */
  def getTopDailyPredictions(config: Map[String, String]): DataFrame = {

    getDailyPredictions(config)
      .withColumn("rank", row_number()
        .over(Window
          .partitionBy("year", "month", "mdate")
          .orderBy(desc("daily_count"))))
      .filter(col("rank").leq(10))
      .withColumn("longitude", col("location.longitude"))
      .withColumn("latitude", col("location.latitude"))
      .withColumn("human_address", col("location.human_address"))
      .drop("rank", "location")

    //SQL Equivalent
    //    sparkSession.sql(
    //      """
    //        |select sb.gps_location,sb.year,sb.month, sb.mdate, sb.daily_count from (
    //        |select
    //        |concat("(",location.latitude,",",location.longitude,")") as gps_location
    //        |,year
    //        |,month
    //        |,mdate
    //        |,daily_count
    //        |,rank( ) over (partition by year,month,mdate order by daily_count desc) rk
    //        |from dailyPredictions
    //        |) sb where sb.rk<=10
    //        |""".stripMargin
    //    )
  }

}

object GETPredictions {
  def apply(predictionType: String, config: Map[String, String] = Map[String, String]()): DataFrame = {
    val predictios = new GETPredictions()
    predictionType match {
      case "daily" => predictios.getTopDailyPredictions(config)
      case "monthly" => predictios.getTopMonthlyPredictions(config)
      case _ => throw new Exception("Un-Implemented Prediction")
    }
  }
}
