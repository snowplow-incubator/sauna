package com.snowplowanalytics.sauna

import java.io.File

import com.typesafe.config.ConfigFactory

/**
  * Represents project configuration.
  */
case class SaunaConfig(queueName: String, accessKeyId: String,
                       secretAccessKey: String, optimizelyToken: String, saunaRoot: String)

object SaunaConfig {
  def apply(file: File): SaunaConfig = {
    val conf = ConfigFactory.parseFile(file)

    SaunaConfig(
      conf.getString("queue.name"),
      conf.getString("aws.access_key_id"),
      conf.getString("aws.secret_access_key"),
      conf.getString("optimizely.token"),
      conf.getString("sauna.root")
    )
  }
}