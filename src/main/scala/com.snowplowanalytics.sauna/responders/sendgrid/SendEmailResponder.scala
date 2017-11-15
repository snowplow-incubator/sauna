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

// java
import java.io.InputStream

// scala
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{Failure, Success}
import scala.util.control.NonFatal

// akka
import akka.actor.{ActorRef, Props}

// play
import play.api.libs.json.Json
import play.api.libs.json.JsValue

// iglu
import com.snowplowanalytics.iglu.client.SchemaCriterion

// sauna
import apis.Sendgrid
import apis.Sendgrid._
import observers.Observer.{ObserverCommandEvent, ObserverEvent}
import responders.Responder.{ResponderEvent, ResponderResult}
import responders.sendgrid.SendEmailResponder._
import utils._

/**
 * A responder that sends emails using the SendGrid Web API.
 *
 * @see https://github.com/snowplow/sauna/wiki/SendGrid-Responder-user-guide#22-send-email-real-time
 * @param sendgrid An instance of the SendGrid API wrapper
 * @param logger   A logger actor
 */
class SendEmailResponder(sendgrid: Sendgrid, val logger: ActorRef) extends Responder[ObserverCommandEvent, SendgridEmailReceived] {
  // Supports only commands
  def extractEvent(observerEvent: ObserverEvent): Option[SendgridEmailReceived] =
    extractEventFromCommand(observerEvent, criterion, SendgridEmailReceived.apply)

  def process(event: SendgridEmailReceived): Unit =
    sendgrid.sendEmail(event.data).onComplete {
      case Success(message) =>
        if (message.status >= 200 && message.status <= 299)
          context.parent ! SendgridEmailSent(event, s"Sent email via Sendgrid")
        else
          notifyLogger(s"Unexpected response from Sendgrid: ${message.body}")
      case Failure(error) => notifyLogger(s"Error while sending Sendgrid message: $error")
    }
}

object SendEmailResponder {

  val criterion = SchemaCriterion("com.sendgrid.sauna.commands", "send_email", "jsonschema", 1, 0)

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
