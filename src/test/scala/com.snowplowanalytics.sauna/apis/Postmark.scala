/*
 * Copyright (c) 2016-2017 Snowplow Analytics Ltd. All rights reserved.
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
import play.api.libs.ws.WSResponse

// sauna
import utils.wsClient

/**
 * Encapsulates all communication with Postmark - an outbound/inbound email service.
 *
 * @param apiToken The API token.
 */
class Postmark(apiToken: String) {

  import Postmark._

  /**
   * Attempt to retrieve message(s) with a specific subject sent to the inbound server.
   *
   * @param subject The subject of the messages.
   * @return Future WSResponse.
   */
  def getInboundMessage(subject: String): Future[WSResponse] = {
    wsClient.url(urlPrefix + s"messages/inbound?count=1&offset=0&subject=$subject")
      .withHeaders("Accept" -> "application/json", "X-Postmark-Server-Token" -> apiToken)
      .get()
  }
}

object Postmark {
  val urlPrefix = "https://api.postmarkapp.com/"
}
