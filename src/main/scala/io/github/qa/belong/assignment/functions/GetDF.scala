/*
 * Copyright (c) Belong 2022. Not to be shared publicly.
 */
package io.github.qa.belong.assignment.functions

import org.apache.spark.sql.DataFrame

class GetDF extends Session {

  /**
   * method to read data from Rest End poit and load into Dataframe
   *
   * @param url
   * @return dataframe
   * {{{
   *         sparkSession.read.json(Seq(RESTReader(url)).toDS)
   *
   * }}}
   */
  def getDataFromRest(url: String): DataFrame = {
    try {
      import sparkSession.implicits._
      sparkSession.read.json(Seq(RESTReader(url)).toDS)
    } catch {
      case e: Exception => {
        logger.error("Unable to generate Dataframe from given input URL", e)
        throw e
      }
    }
  }
}

object GetDF {
  def apply(url: String): DataFrame = (new GetDF()).getDataFromRest(url)
}
