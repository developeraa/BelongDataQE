/*
 * Copyright (c) Belong 2022. Not to be shared publicly.
 */
package io.github.qa.belong.assignment.functions

import org.apache.spark.sql.SparkSession
import org.slf4j.{Logger, LoggerFactory}

trait Session {
  /** Application logger
   * One place to define it and invoke every where.
   */
  def logger: Logger = LoggerFactory.getLogger(getClass)

  /** session
   * here the spark session initiated with sql Support
   */
  val sparkSession: SparkSession = SparkSession
    .builder()
    .master("local[*]")
    .getOrCreate()
}
