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

// akka
import akka.actor.{Actor, ActorRef}

// awscala
import awscala.s3.{Bucket, S3}
import awscala.sqs.Message

// sauna
import loggers.Logger.Notification
import observers.Observer._
import responders.Responder._

/**
 * Responder actors are responsible for extracting events designated for them
 * from observer-events. After event extracted, responder can delegate it
 * to dedicated worker
 *
 * @tparam OE observer event (input),
 *            this is aux type to mark what events could be accepted
 * @tparam RE responder event (output),
 *            which responder supposed to process
 */
trait Responder[OE <: ObserverEvent, RE <: ResponderEvent] extends Actor {

  /**
   * Common for all responders actor logger to dump errors and warnings
   * Declared as def to prevent trait val initialization error
   */
  def logger: ActorRef

  def receive = {
    // Check if message should be handled by responder and actually process it
    // Mediator awaits for `ResponderAck`
    case message: ObserverEvent =>
      extractEvent(message) match {
        case Some(event) =>
          sender() ! Accepted(message, self)
          process(event)
        case None =>
          sender() ! Rejected(message, self)
      }

    // Forward result to mediator
    case processed: ResponderResult =>
      context.parent ! processed

    // Log message
    case notification: Notification =>
      logger ! notification
  }

  /**
   * Try to extract responder-specific event from observer-specific event
   * If extraction is unsuccessful - this event not supposed to be handled by
   * this responder
   *
   * @param observerEvent some event sent from observer, like "file published"
   * @return Some responder-specific event if this observer-event need to be
   *         processed by this responder, None if event need to be skept
   */
  def extractEvent(observerEvent: ObserverEvent): Option[RE]

  /**
   * Primary responder's method. Process file or delegate job to worker actor.
   *
   * Wherever this method with responder event called, responder (or parent)
   * will want to receive `ResponderResult` with this same event as source
   * somewhere in future
   *
   * @param event describes necessary data about processing event
   * @return future with message about successful or not successful execution
   */
  def process(event: RE): Unit

  /**
   * Log unstructured notification
   */
  def notify(message: String): Unit = {
    logger ! Notification(message)
  }
}

object Responder {

  /**
   * Type signalling to root actor that `Responder` started to process
   * observer event or rejected it. If root actor recieved `Accepted` it will
   * await for `ResponderResult` with same event.
   *
   * This is the only message that root actor can safely ask about
   */
  sealed trait ResponderAck extends Product with Serializable

  case class Rejected(observerEvent: ObserverEvent, responder: ActorRef) extends ResponderAck

  case class Accepted(observerEvent: ObserverEvent, responder: ActorRef) extends ResponderAck

  /**
   * Every responder must be able to process some specific type of event
   * This event must contain a source, which in turn is always observer-event,
   * which can be used to obtain full picture and can be anything that can
   * provide full event's data
   */
  trait ResponderEvent {
    /**
     * Each responder event need to include reference to some source from
     * which whole event data (`ResponderEvent`) can be extracted.
     */
    def source: ObserverEvent
  }

  /**
   * Message denoting end of processing of some responder-specific `ResponderEvent`
   * This should be sent to mediator actor for each accepted observer-event
   */
  trait ResponderResult {
    def message: String

    def source: ResponderEvent
  }

  /**
   * Helper class to represent a bucket from which published file can streamed
   *
   * @param s3     AWS S3 credentials
   * @param bucket AWS S3 bucket
   */
  case class S3Source(s3: S3, bucket: Bucket, message: Message)

}