/*
 * Copyright (c) 2016 Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0,
 * and you may not use this file except in compliance with the Apache License Version 2.0.
 * You may obtain a copy of the Apache License Version 2.0 at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the Apache License Version 2.0 is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the Apache License Version 2.0 for the specific language governing permissions and limitations there under.
 */
package com.snowplowanalytics.sauna
package config

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