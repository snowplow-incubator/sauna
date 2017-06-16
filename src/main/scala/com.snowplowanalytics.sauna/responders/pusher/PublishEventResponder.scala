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
package pusher

// scala
import scala.concurrent.ExecutionContext.Implicits.global
import scala.io.Source
import scala.util.{Failure, Success}

// play
import play.api.libs.json._

// akka
import akka.actor.{ActorRef, Props}

// sauna
import Responder._
import PublishEventResponder._
import apis.Pusher
import apis.Pusher._
import loggers.Logger.Notification
import observers.Observer._
import utils.Command

class PublishEventResponder(pusher: Pusher, val logger: ActorRef) extends Responder[ObserverCommandEvent, EventReceived] {
  
  def extractEvent(observerEvent: ObserverEvent): Option[EventReceived] = {
    observerEvent match {
      case e: ObserverCommandEvent =>
        val commandJson = Json.parse(Source.fromInputStream(e.streamContent).mkString)
        Command.extractCommand[Event](commandJson) match {
          case Right((envelope, data)) =>
            Command.validateEnvelope(envelope) match {
              case None =>
                Some(EventReceived(data, e))
              case Some(error) =>
                logger ! Notification(error)
                None
            }
          case Left(error) =>
            logger ! Notification(error)
            None
        }
      case _ => None
    }
  }

  /**
   * Send a valid event using the API wrapper.
   *
   * @param event The event containing a event.
   */
  def process(event: EventReceived): Unit = {
    val notifyWrapper = (s: String) => logger ! Notification(s"Error while sending Pusher notification: $s") 
    pusher.publishEvent(event.data).onComplete {
      case Success(result) if result.status.isSuccessful  => context.parent ! EventSent(event, s"Successfully sent Pusher notification: $result")
      case Success(result) => notifyWrapper(result.toString)
      case Failure(error) => notifyWrapper(error.toString)
    }
  }
}

object PublishEventResponder {
  case class EventReceived(
    data: Event,
    source: ObserverCommandEvent
  ) extends ResponderEvent

  /**
   * A responder result denoting that a Pusher event was successfully sent
   * by the responder.
   *
   * @param source  The responder event that triggered this.
   * @param message A success message.
   */
  case class EventSent(
    source: EventReceived,
    message: String) extends ResponderResult

  /**
   * Constructs a [[Props]] for a [[SendRoomNotificationResponder]] actor.
   *
   * @param Pusher The Pusher API wrapper.
   * @param logger  A logger actor.
   * @return [[Props]] for the new actor.
   */
  def props(pusher: Pusher, logger: ActorRef): Props =
    Props(new PublishEventResponder(pusher, logger))
}