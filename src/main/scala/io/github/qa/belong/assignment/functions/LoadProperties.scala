/*
 * Copyright (c) Belong 2022. Not to be shared publicly.
 */

package io.github.qa.belong.assignment.functions

import com.amazonaws.ClientConfiguration
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain
import com.amazonaws.regions.Regions
import com.amazonaws.services.s3.{AmazonS3, AmazonS3ClientBuilder, AmazonS3URI}

import java.io.FileInputStream
import java.util.Properties

trait LoadProperties extends Session {

  implicit class LoadProperties(inputProps: String) extends Session {

    def loadParms(appProps: String = "app.properties"): Properties = {
      var props = loadLocalappProps(appProps)
      if (inputProps.nonEmpty) {
        if (inputProps.contains("s3a:") || inputProps.contains("s3:")) {
          val s3Client = getS3Client()
          val S3URI = new AmazonS3URI(inputProps.replaceAll("s3a:", "s3:"))
          props.load(s3Client.getObject(S3URI.getBucket, S3URI.getKey).getObjectContent)
        } else {
          props.load(new FileInputStream(inputProps))
        }
      }
      props
    }

    /**
     * loads Internal properties as baseline for application.
     *
     * @return props java.util.properties
     * {{{
     *                val internalpops = getClass.getClassLoader.getResourceAsStream(appProps)
     *                prop.load(internalpops)
     *
     * }}}
     */
    def loadLocalappProps(appProps: String = "app.properties"): Properties = {

      val prop = new Properties()
      try {
        val internalpops = getClass.getClassLoader.getResourceAsStream(appProps)
        prop.load(internalpops)
      } catch {
        case e: Throwable =>
          logger.warn("Unable to load local properties", e)
          throw e
      }
      prop
    }

    //Load Properties form S3 from persistance store.
    def getS3Client(awsRegion: Regions = Regions.AP_SOUTHEAST_2): AmazonS3 = {
      try {
        val credentialsProvider = DefaultAWSCredentialsProviderChain.getInstance()
        val clientConfiguration = new ClientConfiguration()
        clientConfiguration.setSignerOverride("AWSS3V4SignerType")
        AmazonS3ClientBuilder
          .standard().withCredentials(credentialsProvider)
          .withClientConfiguration(clientConfiguration)
          .withRegion(awsRegion)
          .build()
      } catch {
        case e: Exception => {
          logger.error("Unable to crease S3Client..")
          throw e
        }
      }
    }
  }

}
