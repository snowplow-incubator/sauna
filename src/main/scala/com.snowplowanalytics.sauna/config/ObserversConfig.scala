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

case class ObserversConfig(localObserverEnabled: Boolean,
                           s3ObserverEnabled: Boolean,
                           saunaRoot: String,
                           awsRegion: String,
                           awsAccessKeyId: String,
                           awsSecretAccessKey: String,
                           sqsName: String
                          )

object ObserversConfig {
  def apply(observersDirectory: String): ObserversConfig = {
    val localFile = new File(s"$observersDirectory/local_filesystem_config.json")
    val s3File = new File(s"$observersDirectory/aws_s3_observer_config.json")

    lazy val localJson = Json.parse(fromFile(localFile).mkString)
    lazy val s3Json = Json.parse(fromFile(s3File).mkString)

    val localObserverEnabled = if (localFile.exists()) (localJson \ "data" \ "enabled").as[Boolean]
                               else false
    val s3ObserverEnabled = if (s3File.exists()) (s3Json \ "data" \ "enabled").as[Boolean]
                            else false

    if (!localObserverEnabled && !s3ObserverEnabled)
      throw new RuntimeException("At least one observer should be enabled.")

    val saunaRoot = if (localObserverEnabled) (localJson \ "data" \ "parameters" \ "saunaRoot").as[String]
                    else "/tmp" // fallback
    val awsRegion = if (s3ObserverEnabled) (s3Json \ "data" \ "parameters" \ "awsRegion").as[String]
                    else ""
    val awsAccessKeyId = if (s3ObserverEnabled) (s3Json \ "data" \ "parameters" \ "awsAccessKeyId").as[String]
                         else ""
    val awsSecretAccessKey = if (s3ObserverEnabled) (s3Json \ "data" \ "parameters" \ "awsSecretAccessKey").as[String]
                             else ""
    val sqsName = if (s3ObserverEnabled) (s3Json \ "data" \ "parameters" \ "sqsName").as[String]
                  else ""

    new ObserversConfig(
      localObserverEnabled,
      s3ObserverEnabled,
      saunaRoot,
      awsRegion,
      awsAccessKeyId,
      awsSecretAccessKey,
      sqsName
    )
  }
}