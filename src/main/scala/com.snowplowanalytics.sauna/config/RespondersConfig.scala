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

/**
 * Represents project configuration for responders.
 */
case class RespondersConfig(targetingListEnabled: Boolean,
                            dynamicClientProfilesEnabled: Boolean,
                            recipientsEnabled: Boolean,
                            optimizelyImportRegion: String,
                            optimizelyToken: String,
                            sendgridToken: String)

object RespondersConfig {
  def apply(respondersDirectory: String): RespondersConfig = {
    val optimizelyFile = new File(s"$respondersDirectory/optimizely_config.json")
    val sendgridFile = new File(s"$respondersDirectory/sendgrid_config.json")

    lazy val optimizelyJson = Json.parse(fromFile(optimizelyFile).mkString)
    lazy val sendgridJson = Json.parse(fromFile(sendgridFile).mkString)

    val targetingListEnabled = if (optimizelyFile.exists()) (optimizelyJson \ "data" \ "targetingListEnabled").as[Boolean]
                               else false
    val dynamicClientProfilesEnabled = if (optimizelyFile.exists()) (optimizelyJson \ "data" \ "dynamicClientProfilesEnabled").as[Boolean]
                                       else false
    val recipientsEnabled = if (sendgridFile.exists()) (sendgridJson \ "data" \ "recipientsEnabled").as[Boolean]
                            else false

    if (!targetingListEnabled && !dynamicClientProfilesEnabled && !recipientsEnabled)
      throw new RuntimeException("At least one responder should be enabled.")

    val optimizelyImportRegion = if (dynamicClientProfilesEnabled) (optimizelyJson \ "data" \ "parameters" \ "awsRegion").as[String]
                                 else ""
    val optimizelyToken = if (targetingListEnabled || dynamicClientProfilesEnabled) (optimizelyJson \ "data" \ "parameters" \ "token").as[String]
                          else ""
    val sendgridToken = if (recipientsEnabled) (sendgridJson \ "data" \ "parameters" \ "token").as[String]
                        else ""

    new RespondersConfig(
      targetingListEnabled,
      dynamicClientProfilesEnabled,
      recipientsEnabled,
      optimizelyImportRegion,
      optimizelyToken,
      sendgridToken
    )
  }
}