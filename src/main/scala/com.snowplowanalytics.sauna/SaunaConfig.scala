package com.snowplowanalytics.sauna

// java
import java.io.File

// typesafe.config
import com.typesafe.config.ConfigFactory

/**
 * Represents project configuration.
 */
case class SaunaConfig(queueName: String, awsRegion: String, accessKeyId: String, secretAccessKey: String,
                       ddbTableName: String, optimizelyToken: String, optimizelyImportRegion: String,
                       saunaRoot: String, hipchatRoomId: String, hipchatToken: String, sendgridToken: String
                      )

object SaunaConfig {
  def apply(file: File): SaunaConfig = {
    val conf = ConfigFactory.parseFile(file)

    SaunaConfig(
      queueName = conf.getString("aws.sqs.queue.name"),
      awsRegion = conf.getString("aws.region"),
      accessKeyId = conf.getString("aws.access_key_id"),
      secretAccessKey = conf.getString("aws.secret_access_key"),
      ddbTableName = conf.getString("aws.dynamodb.table_name"),
      optimizelyToken = conf.getString("optimizely.token"),
      optimizelyImportRegion = conf.getString("optimizely.import_region"),
      saunaRoot = conf.getString("sauna.root"),
      hipchatRoomId = conf.getString("logging.hipchat.room_id"),
      hipchatToken = conf.getString("logging.hipchat.token"),
      sendgridToken = conf.getString("sendgrid.token")
    )
  }
}