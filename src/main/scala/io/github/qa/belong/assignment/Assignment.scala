/*
 * Copyright (c) Belong 2022. Not to be shared publicly.
 */

package io.github.qa.belong.assignment

import io.github.qa.belong.assignment.functions.{DataframeWriter, GETPredictions, LoadProperties}

import java.util.InputMismatchException
import scala.collection.JavaConverters.mapAsScalaMapConverter

class Assignment extends LoadProperties {

  /**
   * Entry method to Run Application which reads the arguments and process input data.
   *
   * @param args
   */
  def appRunner(args: Array[String]): Unit = {
    if (args.length < 1) {
      logger.error(
        "Required Arguments Not Provided as properties file needed for running application")
      throw new InputMismatchException("Required Arguments Not Provided as properties file needed for running application, must provide property file to process the data")
    }
    val props = args(0).loadParms()
    val mode = props.getProperty("mode")
    val appName = props.getProperty("appName")
    val config: Map[String, String] = props.asInstanceOf[java.util.Map[String, String]].asScala.toMap
    try {
      logger.info("top 10 predictions for each day")
      GETPredictions("daily", config).show(false)

      logger.info("top 10 predictions for each month")
      GETPredictions("monthly", config).show(false)

      logger.info("saving daily predictions into target ")
      DataframeWriter(GETPredictions("daily", config), config ++ Map("targetURI" -> (config("targetURI") + "/daily")))

      logger.info("saving monthly predictions into target ")
      DataframeWriter(GETPredictions("monthly", config), config ++ Map("targetURI" -> (config("targetURI") + "/monthly")))
    } catch {
      case e: Exception => {
        logger.error("Exception Occurred While Processing Data", e)
        throw e
      }
    }
  }
}

object Assignment {
  def apply(args: Array[String]): Unit = (new Assignment()).appRunner(args)
}
