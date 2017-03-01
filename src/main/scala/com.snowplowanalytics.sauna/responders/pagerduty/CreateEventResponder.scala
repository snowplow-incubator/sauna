/*
 * Copyright (c) 2017 Snowplow Analytics Ltd. All rights reserved.
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
package pagerduty

// scala
import scala.concurrent.ExecutionContext.Implicits.global
import scala.io.Source
import scala.util.{Failure, Success}

// akka
import akka.actor.{ActorRef, Props}

// play
import play.api.libs.json.Json

// sauna
import CreateEventResponder._
import Responder._
import apis.PagerDuty
import apis.PagerDuty.PagerDutyEvent
import loggers.Logger.Notification
import observers.Observer.ObserverBatchEvent
import utils.Command

class CreateEventResponder(pagerDuty: PagerDuty, val logger: ActorRef) extends Responder[PagerDutyEventReceived] {
  override def extractEvent(observerEvent: ObserverBatchEvent): Option[PagerDutyEventReceived] = {
    observerEvent.streamContent match {
      case Some(is) =>
        val commandJson = Json.parse(Source.fromInputStream(is).mkString)
        Command.extractCommand[PagerDutyEvent](commandJson) match {
          case Right((envelope, data)) =>
            Command.validateEnvelope(envelope) match {
              case None =>
                Some(PagerDutyEventReceived(data, observerEvent))
              case Some(error) =>
                logger ! Notification(error)
                None
            }
          case Left(error) =>
            logger ! Notification(error)
            None
        }
      case None =>
        logger ! Notification("No stream present, cannot parse command")
        None
    }
  }

  override def process(event: PagerDutyEventReceived): Unit =
    pagerDuty.createEvent(event.data).onComplete {
      case Success(message) =>
        if (message.status == 200)
          context.parent ! PagerDutyEventSent(event, s"Successfully created PagerDuty event: ${message.body}")
        else
          logger ! Notification(s"Unexpected response from PagerDuty: ${message.body}")
      case Failure(error) => logger ! Notification(s"Error while creating PagerDuty event: $error")
    }
}

object CreateEventResponder {
  case class PagerDutyEventReceived(
    data: PagerDutyEvent,
    source: ObserverBatchEvent
  ) extends ResponderEvent[ObserverBatchEvent]

  case class PagerDutyEventSent(
    source: PagerDutyEventReceived,
    message: String) extends ResponderResult

  /**
   * Constructs a [[Props]] for a [[CreateEventResponder]] actor.
   *
   * @param pagerDuty The PagerDuty API wrapper.
   * @param logger    A logger actor.
   * @return [[Props]] for the new actor.
   */
  def props(pagerDuty: PagerDuty, logger: ActorRef): Props =
    Props(new CreateEventResponder(pagerDuty, logger))
}