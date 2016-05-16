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
import scala.concurrent.Future

// play
import play.api.libs.json.JsValue
import play.api.libs.ws.WSResponse

// sauna
import utils._

/**
 * Sendgrid API wrapper. Encapsulates all communications with Sendgrid
 *
 * @param apiKeyId Sendgrid token
 */
class Sendgrid(apiKeyId: String) {
  import Sendgrid._

  /**
   * Tries to get a recipient by id.
   *
   * @param id An recipient id.
   * @return Future[Response]
   */
  def getRecipient(id: String): Future[WSResponse] =
    wsClient.url(urlPrefix + s"contactdb/recipients/$id")
            .withHeaders("Authorization" -> s"Bearer $apiKeyId")
            .get

  /**
   * Tries to upload several recipients. Note that this function is not limited by
   * Sendgrid's limit in 1500 recipients per second, it does what is said to do.
   *
   * @see https://sendgrid.com/docs/API_Reference/Web_API_v3/Marketing_Campaigns/contactdb.html#Add-Single-Recipient-POST
   * @param json JSON supposed to be an array of objects ready to be sent to Sendgrid
   * @return Future[Response]
   */
  def postRecipients(json: JsValue): Future[WSResponse] =
    wsClient.url(urlPrefix + s"contactdb/recipients")
            .withHeaders("Authorization" -> s"Bearer $apiKeyId", "Content-Type" -> "application/json")
            .post(json)

  /**
   * Tries to delete a recipient by id.
   *
   * @param id An recipient id.
   * @return Future[Response]
   */
  def deleteRecipient(id: String): Future[WSResponse] =
    wsClient.url(urlPrefix + s"contactdb/recipients/$id")
            .withHeaders("Authorization" -> s"Bearer $apiKeyId")
            .delete()
}

object Sendgrid {
  val urlPrefix = "https://api.sendgrid.com/v3/"
}