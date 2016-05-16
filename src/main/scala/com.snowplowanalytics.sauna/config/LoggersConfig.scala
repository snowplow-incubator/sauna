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

// scala
import scala.io.Source.fromFile

// play-json
import play.api.libs.json.Json

case class LoggersConfig(hipchatEnabled: Boolean,
                         dynamodbEnabled: Boolean,
                         hipchatRoomId: String,
                         hipchatToken: String,
                         dynamodbTableName: String)

object LoggersConfig {
  def apply(loggersDirectory: String): LoggersConfig = {
    val hipchatFile = new File(s"$loggersDirectory/hipchat_config.json")
    val dynamodbFile = new File(s"$loggersDirectory/dynamodb_config.json")

    lazy val hipchatJson = Json.parse(fromFile(hipchatFile).mkString)
    lazy val dynamodbJson = Json.parse(fromFile(dynamodbFile).mkString)

    val hipchatEnabled = if (hipchatFile.exists()) (hipchatJson \ "data" \ "enabled").as[Boolean]
                         else false
    val dynamodbEnabled = if (dynamodbFile.exists()) (dynamodbJson \ "data" \ "enabled").as[Boolean]
                          else false

    // if both are disabled, StdoutLogger will be used

    val hipchatRoomId = if (hipchatEnabled) (hipchatJson \ "data" \ "parameters" \ "roomId").as[String]
                        else ""
    val hipchatToken = if (hipchatEnabled) (hipchatJson \ "data" \ "parameters" \ "token").as[String]
                       else ""
    val dynamodbTableName = if (dynamodbEnabled) (dynamodbJson \ "data" \ "parameters" \ "dynamodbTableName").as[String]
                            else ""

    new LoggersConfig(
      hipchatEnabled,
      dynamodbEnabled,
      hipchatRoomId,
      hipchatToken,
      dynamodbTableName
    )
  }
}