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
package com.snowplowanalytics.sauna
package apis

// java
import java.util.UUID

// scala
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

// akka
import akka.actor.ActorRef

// play
import play.api.libs.json.Json
import play.api.libs.ws.WSResponse

// jackson
import com.fasterxml.jackson.core.JsonParseException

// sauna
import loggers.Logger.{Notification, Manifestation}
import processors.TargetingList

/**
 * Encapsulates any action with Optimizely.
 */
class Optimizely(implicit logger: ActorRef) extends HasWSClient {
  import Optimizely._

  /**
   * Uploads data to Optimizely.
   *
   * @param tlData Data to be uploaded.
   * @param token Optimizely token.
   */
  def postTargetingLists(tlData: Seq[TargetingList.Data],
                         token: String = Sauna.config.optimizelyToken): Unit = {
    val projectId = tlData.head.projectId // all tls have one projectId

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
                val id = (json \ "id").asOpt[Long]
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
                logger ! Manifestation(id.toString, name, status, description, lastModified)
                if (status == 201) {
                  logger ! Notification(s"Successfully uploaded targeting lists with name [$name].")
                } else {
                  logger ! Notification(s"Unable to upload targeting list with name [$name] : [${response.body}].")
                }

              } catch { case e: JsonParseException =>
                logger ! Manifestation(defaultId, defaultName, status, defaultDescription, defaultLastModified)
                logger ! Notification(s"Problems while connecting to Optimizely API. See [${response.body}].")
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
                                 token: String = Sauna.config.optimizelyToken): Future[Option[(String, String)]] = {
    wsClient.url(urlPrefix + s"dcp_datasources/$dcpDatasourceId")
            .withHeaders("Token" -> token)
            .get()
            .map { case response =>
              try { // response.body is valid json
                val json = Json.parse(response.body)

                for (
                  accessKey <- (json \ "aws_access_key").asOpt[String];
                  secretKey <- (json \ "aws_secret_key").asOpt[String]
                ) yield (accessKey, secretKey)

              } catch { case e: JsonParseException =>
                logger ! Notification(s"Problems while connecting to Optimizely API. See [${response.body}].")
                None
              }
            }
  }

  /**
   * Gets an information about some targeting list.
   *
   * @param tlListId Identifier of the targeting list we are looking for.
   * @param token Optimizely token.
   * @return Future WSResponse.
   */
  def getTargetingList(tlListId: String,
                       token: String = Sauna.config.optimizelyToken): Future[WSResponse] =
    wsClient.url(urlPrefix + s"targeting_lists/$tlListId")
            .withHeaders("Token" -> token)
            .get

  /**
   * Deletes an information about some targeting list.
   *
   * @param tlListId Identifier of the targeting list we are looking for.
   * @param token Optimizely token.
   * @return Future WSResponse.
   */
  def deleteTargetingList(tlListId: String,
                          token: String = Sauna.config.optimizelyToken): Future[WSResponse] =
    wsClient.url(urlPrefix + s"targeting_lists/$tlListId")
            .withHeaders("Token" -> token)
            .delete
}

object Optimizely {
  val urlPrefix = "https://www.optimizelyapis.com/experiment/v1/"
}