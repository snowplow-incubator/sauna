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
import Responder.{ResponderEvent, ResponderResult}
import SendMessageResponder._
import apis.Slack
import apis.Slack._
import loggers.Logger.Notification
import observers.Observer.ObserverBatchEvent
import utils.Command

class SendMessageResponder(slack: Slack, val logger: ActorRef) extends Responder[WebhookMessageReceived] {
  override def extractEvent(observerEvent: ObserverBatchEvent): Option[WebhookMessageReceived] = {
    observerEvent.streamContent match {
      case Some(is) =>
        val commandJson = Json.parse(Source.fromInputStream(is).mkString)
        Command.extractCommand[WebhookMessage](commandJson) match {
          case Right((envelope, data)) =>
            Command.validateEnvelope(envelope) match {
              case None =>
                Some(WebhookMessageReceived(data, observerEvent))
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

  override def process(event: WebhookMessageReceived): Unit =
    slack.sendMessage(event.data).onComplete {
      case Success(message) =>
        if (message.status == 200)
          context.parent ! WebhookMessageSent(event, s"Successfully sent Slack message: $message")
        else
          logger ! Notification(s"Slack message sent but got unexpected response: $message  ")
      case Failure(error) => logger ! Notification(s"Error while sending Slack message: $error")
    }
}

object SendMessageResponder {
  case class WebhookMessageReceived(
    data: WebhookMessage,
    source: ObserverBatchEvent
  ) extends ResponderEvent[ObserverBatchEvent]

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