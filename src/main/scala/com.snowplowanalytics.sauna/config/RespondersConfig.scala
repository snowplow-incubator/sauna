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

import ConfigUtils._

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
    val optimizelyName = "optimizely_config"
    val sendgridName = "sendgrid_responder"

    val optimizelyJsons = configurationFiles(respondersDirectory, optimizelyName)
    val sendgridJsons = configurationFiles(respondersDirectory, sendgridName)

    val targetingListEnabled = findInJsons(optimizelyJsons, Seq("data", "parameters", "targetingListEnabled")).exists(_.as[Boolean])
    val dynamicClientProfilesEnabled = findInJsons(optimizelyJsons, Seq("data", "parameters", "dynamicClientProfilesEnabled")).exists(_.as[Boolean])
    val recipientsEnabled = findInJsons(sendgridJsons, Seq("data", "parameters", "recipientsEnabled")).exists(_.as[Boolean])

    if (!targetingListEnabled && !dynamicClientProfilesEnabled && !recipientsEnabled)
      throw new RuntimeException("At least one responder should be enabled.")

    val optimizelyImportRegion = if (dynamicClientProfilesEnabled) findInJsons(optimizelyJsons, Seq("data", "parameters", "awsRegion")).map(_.as[String]).get
                                 else ""
    val optimizelyToken = if (dynamicClientProfilesEnabled) findInJsons(optimizelyJsons, Seq("data", "parameters", "token")).map(_.as[String]).get
                          else ""
    val sendgridToken = if (recipientsEnabled) findInJsons(sendgridJsons, Seq("data", "parameters", "token")).map(_.as[String]).get
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