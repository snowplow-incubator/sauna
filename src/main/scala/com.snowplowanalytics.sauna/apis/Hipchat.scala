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

// akka
import akka.actor.ActorRef

// play
import play.api.libs.functional.syntax._
import play.api.libs.json._
import play.api.libs.ws.WSResponse

// sauna
import utils._

/**
 * An API wrapper that encapsulates all communication with HipChat.
 *
 * @param authToken HipChat API access token.
 * @param logger    A logger actor.
 */
class Hipchat(authToken: String, logger: ActorRef) {

  import Hipchat._

  /**
   * Sends a notification to a HipChat room.
   *
   * @param notification Notification to be sent.
   * @return Future WSResponse.
   */
  def sendRoomNotification(notification: RoomNotification): Future[WSResponse] = {
    wsClient.url(urlPrefix + s"room/${notification.roomIdOrName}/notification?auth_token=$authToken")
      .withHeaders("Content-Type" -> "application/json")
      .post(Json.toJson(notification))
  }
}

object Hipchat {
  val urlPrefix = "https://api.hipchat.com/v2/"

  /**
   * Represents a valid HipChat room notification.
   */
  case class RoomNotification(
    roomIdOrName: String,
    color: String,
    message: String,
    doNotify: Boolean,
    messageFormat: String
  )

  /**
   * Custom reader for a RoomNotification instance.
   *
   * Converts notify (JSON) to doNotify (case class) due to notify being a
   * reserved Scala identifier, and lowercases the color/messageFormat enums.
   */
  implicit val roomNotificationReads: Reads[RoomNotification] = (
    (JsPath \ "roomIdOrName").read[String] and
      (JsPath \ "color").read[String].map[String](_.toLowerCase) and
      (JsPath \ "message").read[String] and
      (JsPath \ "notify").read[Boolean] and
      (JsPath \ "messageFormat").read[String].map[String](_.toLowerCase)
    ) (RoomNotification.apply _)

  /**
   * Custom writer for a RoomNotification instance.
   *
   * Converts doNotify (case class) to notify (JSON).
   */
  implicit val roomNotificationWrites: Writes[RoomNotification] = (
    (JsPath \ "roomIdOrName").write[String] and
      (JsPath \ "color").write[String] and
      (JsPath \ "message").write[String] and
      (JsPath \ "notify").write[Boolean] and
      (JsPath \ "messageFormat").write[String]
    ) (unlift(RoomNotification.unapply))
}
