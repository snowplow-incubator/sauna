/*
 * Copyright (c) 2016 Snowplow Analytics Ltd. All rights reserved.
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
package loggers

// akka
import akka.actor.{ Actor, ActorRef, Props }

// sauna
import Logger._

/**
 * Root logger actor that can hold zero or more subloggers and route messages
 * between them. It holds `Props`, not `ActorRef`s, therefore on crash it will
 * also restart subloggers
 *
 * @param dynamodbProps DynamoDB `Props`
 * @param hipchatProps Hipchat `Props`
 */
class Logger(dynamodbProps: Option[DynamodbProps], hipchatProps: Option[HipchatProps]) extends Actor {

  val dynamodbLogger: Option[ActorRef] =
    dynamodbProps.map(dynamodb => context.actorOf(dynamodb.props))

  val hipchatLogger: Option[ActorRef] =
    hipchatProps.map(hipchat => context.actorOf(hipchat.props))

  def receive = {
    case notification: Notification =>
      hipchatLogger.foreach { _ ! notification }
      println(s"NOTIFICATION: [${notification.text}]")

    case manifestation: Manifestation =>
      import manifestation._
      dynamodbLogger.foreach { _ ! manifestation }
      println(s"MANIFESTATION: uid = [$uid], name = [$name], status = [$status], description = [$description], lastModified = [$lastModified]")
  }
}


object Logger {

  // Wrapper types to not confuse untyped Props
  case class DynamodbProps(props: Props)
  case class HipchatProps(props: Props)

  /**
   * ADT to represent possible type of messages
   */
  sealed trait LoggingMessage extends Product with Serializable

  /**
   * A (unstructured) message.
   *
   * @param text Text of notification.
   */
  case class Notification(text: String) extends LoggingMessage

  /**
   * A (structured) message.
   * Note that these params describe a schema.
   *
   * @param uid Unique identifier for message.
   * @param name Message header (for example, Optimizely list name).
   * @param status HTTP code for operation result.
   * @param description What happened.
   * @param lastModified Last modification date, if exists, else - operation date.
   */
  case class Manifestation(uid: String, name: String, status: Int, description: String, lastModified: String) extends LoggingMessage

  def props(dynamodbProps: Option[DynamodbProps], hipchatProps: Option[HipchatProps]): Props =
    Props(new Logger(dynamodbProps, hipchatProps))
}