/*
 * Copyright (c) 2015 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.sauna.apis

import java.util.UUID

import com.fasterxml.jackson.core.JsonParseException
import com.snowplowanalytics.sauna.loggers.Logger
import com.snowplowanalytics.sauna.processors.TargetingList
import com.snowplowanalytics.sauna.{HasWSClient, Sauna}
import play.api.libs.json.Json

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

/**
  * Encapsulates any action with Optimizely.
  */
class Optimizely extends HasWSClient { self: Logger =>
  import Optimizely._

  /**
    * Uploads data to Optimizely.
    *
    * @param tlData Data to be uploaded.
    * @param token Optimizely token.
    */
  def targetingLists(tlData: Seq[TargetingList.Data],
                     token: String = Sauna.saunaConfig.optimizelyToken): Unit = {
    val projectId = tlData.head.projectId // all tls have one projectId

    println(s"tlData = $tlData")

    wsClient.url(urlPrefix + s"projects/$projectId/targeting_lists/")
            .withHeaders("Token" -> token, "Content-Type" -> "application/json")
            .post(TargetingList.merge(tlData))
            .foreach { case response =>
              // those are lazy to emphasize their pattern of use, probably simple `val` would be a bit more efficient
              lazy val defaultId = UUID.randomUUID().toString
              lazy val defaultName = "Not found."
              lazy val defaultDescription = response.body
              lazy val defaultLastModified = new java.sql.Timestamp(System.currentTimeMillis).toString
              val status = response.status

              try { // response.body is valid json
                val json = Json.parse(response.body)
                val id = (json \ "id").asOpt[String]
                                      .orElse((json \ "uuid").asOpt[String])
                                      .getOrElse(defaultId)
                val name = (json \ "name").asOpt[String]
                                          .getOrElse(defaultName)
                val description = (json \ "description").asOpt[String]
                                                        .orElse((json \ "message").asOpt[String])
                                                        .getOrElse(defaultDescription)
                val lastModified = (json \ "last_modified").asOpt[String]
                                                           .getOrElse(defaultLastModified)

                // log results
                self.manifestation(id, name, status, description, lastModified)
                if (status == 201) {
                  self.notification(s"Successfully uploaded targeting lists with name [$name].")
                } else {
                  self.notification(s"Unable to upload targeting list with name [$name] : [${response.body}].")
                }

              } catch { case e: JsonParseException =>
                self.manifestation(defaultId, defaultName, status, defaultDescription, defaultLastModified)
                self.notification(s"Problems while connecting to Optimizely API. See [${response.body}].")
              }
            }
  }

  /**
    * Tries to get credentials for S3 bucket "optimizely-import".
    *
    * @param dcpDatasourceId Your Dynamic Customer Profile datasource id.
    * @param token Optimizely secret token.
    * @return Future Option (awsAccessKey, awsSecretKey) for S3 bucket "optimizely-import"
    */
  def getOptimizelyS3Credentials(dcpDatasourceId: String,
                                 token: String = Sauna.saunaConfig.optimizelyToken): Future[Option[(String, String)]] = {
    wsClient.url(urlPrefix + s"dcp_datasources/$dcpDatasourceId")
            .withHeaders("Token" -> token)
            .get()
            .map { case response =>
              try { // response.body is valid json
                val json = Json.parse(response.body)

                for (
                  accessKey <- (json \ "aws_access_key").asOpt[String];
                  secretKey <- (json \ "aws_access_key").asOpt[String]
                ) yield (accessKey, secretKey)

              } catch { case e: JsonParseException =>
                self.notification(s"Problems while connecting to Optimizely API. See [${response.body}].")
                None
              }
            }
  }
}

object Optimizely {
  val urlPrefix = "https://www.optimizelyapis.com/experiment/v1/"
}