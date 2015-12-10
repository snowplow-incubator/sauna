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

import ConfigUtils._

case class LoggersConfig(hipchatEnabled: Boolean,
                         dynamodbEnabled: Boolean,
                         stdoutEnabled: Boolean,
                         hipchatRoomId: String,
                         hipchatToken: String,
                         dynamodbTableName: String,
                         dynamodbAwsRegion: String,
                         dynamodbAwsAccessKeyId: String,
                         dynamodbAwsSecretAccessKey: String)

object LoggersConfig {
  def apply(loggersDirectory: String): LoggersConfig = {
    val hipchatName = "hipchat_logger"
    val dynamodbName = "dynamodb_logger"
    val stdoutName = "stdout_logger"

    val hipchatJsons = configurationFiles(loggersDirectory, hipchatName)
    val dynamodbJsons = configurationFiles(loggersDirectory, dynamodbName)
    val stdoutJsons = configurationFiles(loggersDirectory, stdoutName)

    val hipchatEnabled = findInJsons(hipchatJsons, Seq("data", "enabled")).exists(_.as[Boolean])
    val dynamodbEnabled = findInJsons(dynamodbJsons, Seq("data", "enabled")).exists(_.as[Boolean])
    val stdoutEnabled = findInJsons(stdoutJsons, Seq("data", "enabled")).exists(_.as[Boolean])

    val hipchatRoomId = if (hipchatEnabled) findInJsons(hipchatJsons, Seq("data", "parameters", "roomId")).map(_.as[String]).get
                        else ""
    val hipchatToken = if (hipchatEnabled) findInJsons(hipchatJsons, Seq("data", "parameters", "token")).map(_.as[String]).get
                       else ""
    val dynamodbTableName = if (dynamodbEnabled) findInJsons(dynamodbJsons, Seq("data", "parameters", "dynamodbTableName")).map(_.as[String]).get
                            else ""
    val dynamodbAwsRegion = if (dynamodbEnabled) findInJsons(dynamodbJsons, Seq("data", "parameters", "dynamodbAwsRegion")).map(_.as[String]).get
                            else ""
    val dynamodbAwsAccessKeyId = if (dynamodbEnabled) findInJsons(dynamodbJsons, Seq("data", "parameters", "dynamodbAwsAccessKeyId")).map(_.as[String]).get
                                 else ""
    val dynamodbAwsSecretAccessKey = if (dynamodbEnabled) findInJsons(dynamodbJsons, Seq("data", "parameters", "dynamodbAwsSecretAccessKey")).map(_.as[String]).get
                                     else ""

    new LoggersConfig(
      hipchatEnabled,
      dynamodbEnabled,
      stdoutEnabled,
      hipchatRoomId,
      hipchatToken,
      dynamodbTableName,
      dynamodbAwsRegion,
      dynamodbAwsAccessKeyId,
      dynamodbAwsSecretAccessKey
    )
  }
}