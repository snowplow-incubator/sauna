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

// akka
import akka.actor.ActorRef

// play
import play.api.libs.ws.WSResponse

// sauna
import processors.sendgrid.Recipients

/**
 * Encapsulates any action with Sendgrid.
 *
 * @param token Sendgrid token.
 * @param logger A logger actor.
 */
class Sendgrid(token: String)
              (implicit logger: ActorRef) extends HasWSClient {
  import Sendgrid._

  /**
   * Tries to get a recipient by id.
   *
   * @param id An recipient id.
   * @return Future[Response]
   */
  def getRecipient(id: String): Future[WSResponse] =
    wsClient.url(urlPrefix + s"contactdb/recipients/$id")
                .withHeaders("Authorization" -> s"Bearer $token")
                .get

  /**
   * Tries to upload several recipients. Note that this function is not limited by
   * Sendgrid's limit in 1500 recipients per second, it does what is said to do.
   *
   * @param keys Seq of attribute keys, repeated for each recipient from `valuess`.
   * @param valuess Seq of recipients, where recipient is a seq of attribute values.
   *                Each `values` in `valuess` should have one length with `keys`.
   * @return Future[Response]
   *
   * @see Sendgrid.makeValidJson
   */
  def postRecipients(keys: Seq[String], valuess: Seq[Seq[String]]): Future[WSResponse] =
    wsClient.url(urlPrefix + s"contactdb/recipients")
            .withHeaders("Authorization" -> s"Bearer $token", "Content-Type" -> "application/json")
            .post(Recipients.makeValidJson(keys, valuess))

  /**
   * Tries to delete a recipient by id.
   *
   * @param id An recipient id.
   * @return Future[Response]
   */
  def deleteRecipient(id: String): Future[WSResponse] =
    wsClient.url(urlPrefix + s"contactdb/recipients/$id")
            .withHeaders("Authorization" -> s"Bearer $token")
            .delete()
}

object Sendgrid {
  val urlPrefix = "https://api.sendgrid.com/v3/"
}