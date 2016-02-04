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
package apis

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
import loggers.Logger.Notification
import responders.optimizely.TargetingList
import utils._

/**
 * Encapsulates any action with Optimizely.
 *
 * @param token Optimizely token.
 * @param logger A logger actor.
 */
class Optimizely(token: String)
                (implicit logger: ActorRef) {
  import Optimizely._

  /**
   * Uploads data to Optimizely.
   *
   * @param tlData Data to be uploaded.
   */
  def postTargetingLists(tlData: Seq[TargetingList.Data]): Future[WSResponse] = {
    val projectId = tlData.head.projectId // all tls have one projectId

    wsClient.url(urlPrefix + s"projects/$projectId/targeting_lists/")
            .withHeaders("Token" -> token, "Content-Type" -> "application/json")
            .post(TargetingList.merge(tlData))
  }

  /**
   * Tries to get credentials for S3 bucket "optimizely-import".
   *
   * @param dcpDatasourceId Your Dynamic Customer Profile datasource id.
   * @return Future Option (awsAccessKey, awsSecretKey) for S3 bucket "optimizely-import"
   */
  def getOptimizelyS3Credentials(dcpDatasourceId: String): Future[Option[(String, String)]] = {
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
   * @return Future WSResponse.
   */
  def getTargetingList(tlListId: String): Future[WSResponse] =
    wsClient.url(urlPrefix + s"targeting_lists/$tlListId")
            .withHeaders("Token" -> token)
            .get

  /**
   * Deletes an information about some targeting list.
   *
   * @param tlListId Identifier of the targeting list we want to delete.
   * @return Future WSResponse.
   */
  def deleteTargetingList(tlListId: String): Future[WSResponse] =
    wsClient.url(urlPrefix + s"targeting_lists/$tlListId")
            .withHeaders("Token" -> token)
            .delete

  /**
   * Deletes an information about some DCP Service.
   *
   * @param datasourceId Identifier of the datasource we want to delete.
   * @return Future WSResponse.
   */
  def deleteDcpService(datasourceId: String): Future[WSResponse] =
    wsClient.url(urlPrefix + s"dcp_services/$datasourceId")
            .withHeaders("Token" -> token)
            .delete
}

object Optimizely {
  val urlPrefix = "https://www.optimizelyapis.com/experiment/v1/"
}