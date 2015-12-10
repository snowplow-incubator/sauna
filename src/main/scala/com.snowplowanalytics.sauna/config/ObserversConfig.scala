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
    val s3Name = "amazon_s3_observer"
    val localName = "local_filesystem_observer"

    val s3Jsons = configurationFiles(observersDirectory, s3Name)
    val localJsons = configurationFiles(observersDirectory, localName)

    val s3ObserverEnabled = findInJsons(s3Jsons, Seq("data", "enabled")).exists(_.as[Boolean])
    val localObserverEnabled = findInJsons(localJsons, Seq("data", "enabled")).exists(_.as[Boolean])

    if (!localObserverEnabled && !s3ObserverEnabled)
      throw new RuntimeException("At least one observer should be enabled.")

    val saunaRoot = if (localObserverEnabled) findInJsons(localJsons, Seq("data", "parameters", "saunaRoot")).map(_.as[String]).get
                    else "/tmp" // fallback
    val awsRegion = if (s3ObserverEnabled) findInJsons(s3Jsons, Seq("data", "parameters", "awsRegion")).map(_.as[String]).get
                    else ""
    val awsAccessKeyId = if (s3ObserverEnabled) findInJsons(s3Jsons, Seq("data", "parameters", "awsAccessKeyId")).map(_.as[String]).get
                         else ""
    val awsSecretAccessKey = if (s3ObserverEnabled) findInJsons(s3Jsons, Seq("data", "parameters", "awsSecretAccessKey")).map(_.as[String]).get
                             else ""
    val sqsName = if (s3ObserverEnabled) findInJsons(s3Jsons, Seq("data", "parameters", "sqsName")).map(_.as[String]).get
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