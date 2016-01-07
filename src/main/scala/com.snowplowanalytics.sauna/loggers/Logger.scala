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
import akka.actor.Actor

trait Logger extends Actor {
  import Logger._

  override def receive = {
    case message: Notification => log(message)
    case message: Manifestation => log(message)
  }

  def log(message: Notification): Unit

  def log(message: Manifestation): Unit
}

object Logger {
  /**
   * A (unstructured) message.
   *
   * @param text Text of notification.
   */
  case class Notification(text: String)

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
  case class Manifestation(uid: String, name: String, status: Int, description: String, lastModified: String)
}