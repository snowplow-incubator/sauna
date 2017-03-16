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
package sendgrid

// scala
import scala.concurrent.ExecutionContext.Implicits.global
import scala.io.Source
import scala.util.{Failure, Success}

// akka
import akka.actor.{ActorRef, Props}

// play
import play.api.libs.json.Json

// sauna
import Responder.{ResponderEvent, ResponderResult}
import SendEmailResponder._
import apis.Sendgrid
import apis.Sendgrid.SendgridEmail
import loggers.Logger.Notification
import observers.Observer.{ObserverCommandEvent, ObserverEvent}
import utils.Command

class SendEmailResponder(sendgrid: Sendgrid, val logger: ActorRef) extends Responder[ObserverCommandEvent, SendgridEmailReceived] {
  def extractEvent(observerEvent: ObserverEvent): Option[SendgridEmailReceived] = {
    observerEvent match {
      case e: ObserverCommandEvent =>
        val commandJson = Json.parse(Source.fromInputStream(e.streamContent).mkString)
        Command.extractCommand[SendgridEmail](commandJson) match {
          case Right((envelope, data)) =>
            Command.validateEnvelope(envelope) match {
              case None =>
                Some(SendgridEmailReceived(data, e))
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

  def process(event: SendgridEmailReceived): Unit =
    sendgrid.sendEmail(event.data).onComplete {
      case Success(message) =>
        if (message.status >= 200 && message.status <= 299)
          context.parent ! SendgridEmailSent(event, s"Successfully sent Sendgrid email!")
        else
          logger ! Notification(s"Unexpected response from Sendgrid: ${message.body}")
      case Failure(error) => logger ! Notification(s"Error while sending Sendgrid message: $error")
    }
}

object SendEmailResponder {
  case class SendgridEmailReceived(
    data: SendgridEmail,
    source: ObserverCommandEvent
  ) extends ResponderEvent

  case class SendgridEmailSent(
    source: SendgridEmailReceived,
    message: String) extends ResponderResult

  /**
   * Constructs a [[Props]] for a [[SendEmailResponder]] actor.
   *
   * @param sendgrid The SendGrid API wrapper.
   * @param logger   A logger actor.
   * @return [[Props]] for the new actor.
   */
  def props(sendgrid: Sendgrid, logger: ActorRef): Props =
    Props(new SendEmailResponder(sendgrid, logger))
}