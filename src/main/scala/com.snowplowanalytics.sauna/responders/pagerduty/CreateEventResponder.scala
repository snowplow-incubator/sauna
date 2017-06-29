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
package pagerduty

// scala
import com.snowplowanalytics.iglu.client.SchemaCriterion

import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{Failure, Success}

// akka
import akka.actor.{ActorRef, Props}

// iglu
import com.snowplowanalytics.iglu.client.SchemaCriterion

// sauna
import apis.PagerDuty
import apis.PagerDuty.PagerDutyEvent
import loggers.Logger.Notification
import observers.Observer._
import responders.Responder._
import responders.pagerduty.CreateEventResponder._

/**
 * A responder that sends events to PagerDuty.
 *
 * @see https://github.com/snowplow/sauna/wiki/PagerDuty-Responder-user-guide#21-send-message-real-time
 * @param pagerDuty An instance of the PagerDuty API wrapper
 * @param logger    A logger actor
 */
class CreateEventResponder(pagerDuty: PagerDuty, val logger: ActorRef) extends Responder[ObserverCommandEvent, PagerDutyEventReceived] {
  // Supports only commands
  def extractEvent(observerEvent: ObserverEvent): Option[PagerDutyEventReceived] =
    extractEventFromCommand(observerEvent, criterion, PagerDutyEventReceived.apply)

  def process(event: PagerDutyEventReceived): Unit =
    pagerDuty.createEvent(event.data).onComplete {
      case Success(message) =>
        if (message.status == 200)
          context.parent ! PagerDutyEventSent(event, s"Created PagerDuty event: ${message.body}")
        else
          logger ! Notification(s"Unexpected response from PagerDuty: ${message.body}")
      case Failure(error) => logger ! Notification(s"Error while creating PagerDuty event: $error")
    }
}

object CreateEventResponder {

  val criterion = SchemaCriterion("com.pagerduty.sauna.commands", "create_event", "jsonschema", 1, 0)

  case class PagerDutyEventReceived(
    data: PagerDutyEvent,
    source: ObserverCommandEvent
  ) extends ResponderEvent

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
