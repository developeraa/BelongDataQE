/*
 * Copyright (c) Belong 2022. Not to be shared publicly.
 */
package io.github.qa.belong.assignment.functions

import okhttp3.{OkHttpClient, Request}

class RESTReader extends Session {

  /**
   * HTTP Client builder
   * {{{
   *   new OkHttpClient().newBuilder().build()
   * }}}
   *
   * @return
   */
  def getClient: OkHttpClient = new OkHttpClient().newBuilder().build()

  /**
   * REST Request builder
   *
   * @param endpoint
   * @param body
   * @return
   * {{{
   *   new Request.Builder()
   *.url(endpoint)
   *.method("GET", null)
   *.build()
   * }}}
   *
   */
  def getRequestBuilder(endpoint: String, body: String = ""): Request = {

    new Request.Builder()
      .url(endpoint)
      .method("GET", null)
      .build()
  }

  /**
   * Utility method to read data from REST pass the output as string
   *
   * @param endpoint
   * @param body
   * @return
   * {{{
   *  getClient.newCall(getRequestBuilder(endpoint, body)).execute().body().string()
   * }}}
   */
  def getResponse(endpoint: String, body: String = ""): String = {
    try {
      getClient.newCall(getRequestBuilder(endpoint, body)).execute().body().string()
    } catch {
      case e: Exception => {
        logger.error(s"Unable to read data from REST Endpoint ${endpoint}", e)
        throw e
      }
    }
  }

}

object RESTReader {
  def apply(endpoint: String): String = (new RESTReader()).getResponse(endpoint)
}

