/*
 * Copyright (c) 2015 Snowplow Analytics Ltd. All rights reserved.
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
import akka.actor.{Props, ActorSystem}

// sauna
import loggers.Logger._

/**
 * Writes messages to standard output.
 */
class StdoutLogger extends Logger {

  override def log(message: Notification): Unit = {
    import message._

    println(s"NOTIFICATION: got message = [$text].")
  }

  override def log(message: Manifestation): Unit = {
    import message._

    println(s"MANIFESTATION: got uid = [$uid], name = [$name], status = [$status], description = [$description], lastModified = [$lastModified].")
  }
}

object StdoutLogger {
  /**
   * Constructs an actor wrapper over Logger.
   *
   * @param system Actor system for the wrapper.
   * @return LoggerActorWrapper.
   */
  def apply(implicit system: ActorSystem): LoggerActorWrapper = {
    new LoggerActorWrapper(system.actorOf(Props(new StdoutLogger)))
  }
}