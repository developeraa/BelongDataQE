/*
 * Copyright (c) Belong 2022. Not to be shared publicly.
 */
package io.github.qa.belong.assignment

import io.github.qa.belong.assignment.functions.Session

object Launcher extends Session {
  /**
   * Entrypoint for Main Application
   *
   * @param args Input arguments for application to run primaryly the config file
   * @return Unit
   */
  def main(args: Array[String]): Unit = {

    try {
      Assignment(args)
      if (sparkSession.conf.getAll.getOrElse("spark.master", "").contains("k8s:")) {
        sparkSession.stop()
      }
    } catch {
      case e: Exception => {
        logger.error("Exception occurred wile running Application..")
        if (sparkSession.conf.getAll.getOrElse("spark.master", "").contains("k8s:")) {
          sparkSession.stop()
        }
        throw e
      }
    }
  }
}
