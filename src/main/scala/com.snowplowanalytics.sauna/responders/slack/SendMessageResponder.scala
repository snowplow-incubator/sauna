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
package slack

// scala
import scala.concurrent.ExecutionContext.Implicits.global
import scala.io.Source
import scala.util.{Failure, Success}

// akka
import akka.actor.{ActorRef, Props}

// play
import play.api.libs.json.Json

// Sauna
import apis.Slack
import apis.Slack._
import observers.Observer._
import responders.Responder.{ResponderEvent, ResponderResult}
import responders.slack.SendMessageResponder._
import utils.Command

/**
 * A responder that sends messages to a Slack channel or user.
 *
 * @see https://github.com/snowplow/sauna/wiki/Slack-Responder-user-guide#21-send-message-real-time
 * @param slack An instance of the HipChat API wrapper
 * @param logger  A logger actor
 */
class SendMessageResponder(slack: Slack, val logger: ActorRef) extends Responder[ObserverCommandEvent, WebhookMessageReceived] {
  def extractEvent(observerEvent: ObserverEvent): Option[WebhookMessageReceived] = {
    observerEvent match {
      case e: ObserverCommandEvent =>
        try {
          val commandJson = Json.parse(Source.fromInputStream(e.streamContent).mkString)
          Command.extractCommand[WebhookMessage](commandJson) match {
            case Right((envelope, data)) =>
              Command.validateEnvelope(envelope) match {
                case Right(_) =>
                  Some(WebhookMessageReceived(data, e))
                case Left(error) =>
                  notifyLogger(error)
                  None
              }
            case Left(error) =>
              notifyLogger(error)
              None
          }
        } catch {
          case e: Exception =>
            notifyLogger("Error in Slack event: " + e.getMessage)
            None
        }
      case _ => None
    }
  }

  def process(event: WebhookMessageReceived): Unit =
    slack.sendMessage(event.data).onComplete {
      case Success(message) =>
        if (message.status == 200)
          context.parent ! WebhookMessageSent(event, s"Successfully sent Slack message: $message")
        else
          notifyLogger(s"Slack message sent but got unexpected response: $message")
      case Failure(error) => notifyLogger(s"Error while sending Slack message: $error")
    }
}

object SendMessageResponder {

  case class WebhookMessageReceived(
    data: WebhookMessage,
    source: ObserverCommandEvent
  ) extends ResponderEvent

  case class WebhookMessageSent(
    source: WebhookMessageReceived,
    message: String) extends ResponderResult

  /**
   * Constructs a [[Props]] for a [[SendMessageResponder]] actor.
   *
   * @param slack  The Slack API wrapper.
   * @param logger A logger actor.
   * @return [[Props]] for the new actor.
   */
  def props(slack: Slack, logger: ActorRef): Props =
    Props(new SendMessageResponder(slack, logger))
}
