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
package responders
package hipchat

// scala
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{Failure, Success}

// akka
import akka.actor.{ActorRef, Props}

// iglu
import com.snowplowanalytics.iglu.client.SchemaCriterion

// sauna
import apis.Hipchat
import apis.Hipchat._
import observers.Observer._
import responders.Responder._
import responders.hipchat.SendRoomNotificationResponder._

/**
 * A responder that sends notifications to a room in HipChat.
 *
 * @see https://github.com/snowplow/sauna/wiki/HipChat-Responder-user-guide#21-send-room-notification-real-time
 * @param hipchat An instance of the HipChat API wrapper
 * @param logger  A logger actor
 */
class SendRoomNotificationResponder(hipchat: Hipchat, val logger: ActorRef) extends Responder[ObserverCommandEvent, RoomNotificationReceived] {
  // Supports only commands
  def extractEvent(observerEvent: ObserverEvent): Option[RoomNotificationReceived] =
    extractEventFromCommand(observerEvent, criterion, RoomNotificationReceived.apply)

  /**
   * Send a valid room notification using the API wrapper.
   *
   * @param event The event containing a room notification.
   */
  def process(event: RoomNotificationReceived): Unit =
    hipchat.sendRoomNotification(event.data).onComplete {
      case Success(message) =>
        if (message.status >= 200 && message.status <= 204)
          context.parent ! RoomNotificationSent(event, s"Sent HipChat notification: ${message.status}")
        else
          notifyLogger(s"Unexpected response from HipChat: ${message.body}")
      case Failure(error) => notifyLogger(s"Error while sending HipChat notification: $error")
    }
}

object SendRoomNotificationResponder {

  val criterion = SchemaCriterion("com.hipchat.sauna.commands", "send_room_notification", "jsonschema", 1, 0)

  case class RoomNotificationReceived(
    data: RoomNotification,
    source: ObserverCommandEvent
  ) extends ResponderEvent

  /**
   * A responder result denoting that a HipChat room notification was successfully sent
   * by the responder.
   *
   * @param source  The responder event that triggered this.
   * @param message A success message.
   */
  case class RoomNotificationSent(
    source: RoomNotificationReceived,
    message: String) extends ResponderResult

  /**
   * Constructs a [[Props]] for a [[SendRoomNotificationResponder]] actor.
   *
   * @param hipchat The HipChat API wrapper.
   * @param logger  A logger actor.
   * @return [[Props]] for the new actor.
   */
  def props(hipchat: Hipchat, logger: ActorRef): Props =
    Props(new SendRoomNotificationResponder(hipchat, logger))
}
