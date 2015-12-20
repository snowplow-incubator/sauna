package com.snowplowanalytics.sauna

import java.io.File

import com.typesafe.config.ConfigFactory

/**
  * Represents project configuration.
  */
case class SaunaConfig(queueName: String, accessKeyId: String,
                       secretAccessKey: String, ddbTableName: String, optimizelyToken: String,
                       saunaRoot: String, hipchatRoomId: String, hipchatToken: String)

object SaunaConfig {
  def apply(file: File): SaunaConfig = {
    val conf = ConfigFactory.parseFile(file)

    SaunaConfig(
      queueName = conf.getString("queue.name"),
      accessKeyId = conf.getString("aws.access_key_id"),
      secretAccessKey = conf.getString("aws.secret_access_key"),
      ddbTableName = conf.getString("aws.dynamodb.table_name"),
      optimizelyToken = conf.getString("optimizely.token"),
      saunaRoot = conf.getString("sauna.root"),
      hipchatRoomId = conf.getString("logging.hipchat.room_id"),
      hipchatToken = conf.getString("logging.hipchat.token")
    )
  }
}