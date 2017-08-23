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
package opsgenie


// scala
import scala.concurrent.ExecutionContext.Implicits.global
import scala.language.postfixOps
import scala.concurrent.Future
import scala.io.Source
import scala.io.Source.fromInputStream
import scala.util.control.NonFatal
import scala.util.{Failure, Success}
import scala.util.{Right, Left}

// akka
import akka.actor.{ActorRef, Props}

// play
import play.api.libs.json._

// sauna
import loggers.Logger.Notification
import CreateAlertResponder._
import apis.OpsGenie
import observers.Observer._
import responders.Responder._
import utils._


/**
 * Responder for creating OpsGenie alerts.
 *
 * @param opsgenie   instance of OpsGenie API Wrapper
 * @param logger       A logger actor
 */
class CreateAlertResponder(opsgenie: OpsGenie, val logger: ActorRef) extends Responder[ObserverCommandEvent, CreateAlertReceived] {
    import OpsGenie._

    def extractEvent(observerEvent: ObserverEvent): Option[CreateAlertReceived] = {
      observerEvent match {
        case e: ObserverCommandEvent =>
          val commandJson = Json.parse(Source.fromInputStream(e.streamContent).mkString)
          Command.extractCommand[Alert](commandJson) match {
            case Right((envelope, data)) =>
              Command.validateEnvelope(envelope) match {
                case Right(_) =>
                  Some(CreateAlertReceived(data, e))
                case Left(error) =>
                  notifyLogger(error)
                  None
              }
            case Left(error) =>
              notifyLogger(error)
              None
          }
        case _ => None
      }
  }

  /**
   * Create a new opsgenie alert using the API wrapper.
   *
   * @param event The event containing an alert.
   */
  def process(event: CreateAlertReceived): Unit =
    opsgenie.createAlert(event.data).onComplete {
      case Success(message) => context.parent ! CreateAlertSent(event, s"Successfully created OpsGenie alert: $message")
      case Failure(error) => logger ! Notification(s"Error while creating OpsGenie alert: $error")
    }

}

object CreateAlertResponder {
  import apis.OpsGenie._
  case class CreateAlertReceived(
    data: Alert,
    source: ObserverCommandEvent
  ) extends ResponderEvent

  /**
   * A responder result denoting that a OpsGenie alert was successfully created
   * by the responder.
   *
   * @param source  The responder event that triggered this.
   * @param message A success message.
   */
  case class CreateAlertSent(
    source: CreateAlertReceived,
    message: String) extends ResponderResult

  /**
   * Constructs a [[Props]] for a [[CreateAlertResponder]] actor.
   *
   * @param opsgenie The OpsGenie API wrapper.
   * @param logger  A logger actor.
   * @return [[Props]] for the new actor.
   */
  def props(opsgenie: OpsGenie, logger: ActorRef): Props =
    Props(new CreateAlertResponder(opsgenie, logger))
}