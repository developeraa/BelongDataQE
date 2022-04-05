/*
 * Copyright (c) Belong 2022. Not to be shared publicly.
 */

package io.github.qa.belong.assignment.functions

import org.apache.spark.sql.DataFrame

class DataframeWriter extends Session {

  /**
   * Data writer to target area
   *
   * @param dataFrame
   * @param options
   * @return - None
   * {{{
   *  dataFrame.repartition(1)
   *.write
   *.format(options.getOrElse("writerFormat","parquet"))
   *.options(options)
   *.mode(options.getOrElse("mode","overwrite"))
   *.save(options("targetURI"))
   * }}}
   */
  def writeData(dataFrame: DataFrame, options: Map[String, String] = Map[String, String]()): Unit = {
    try {
      logger.info(s"Wirting data into target URI: ${options.get("targetURI")}")
      dataFrame.repartition(1)
        .write
        .format(options.getOrElse("writerFormat", "parquet"))
        .options(options)
        .mode(options.getOrElse("writeMode", "overwrite"))
        .save(options("targetURI"))
    } catch {
      case e: Exception => {
        logger.error(s"Unable to write data into Target URI:  ${options.getOrElse("targetURI", "NOT_PROVIDED")}, options: ${options}", e)
        throw e
      }
    }
  }
}

object DataframeWriter {
  def apply(dataFrame: DataFrame, options: Map[String, String] = Map[String, String]()): Unit = new DataframeWriter().writeData(dataFrame, options)
}
