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
package loggers

// akka
import akka.actor.{ Actor, Props }

// sauna
import loggers.Logger._
import utils._

/**
 * Sends notification to some HipChat room
 */
class HipchatLogger(token: String, roomId: String) extends Actor {
  import HipchatLogger._

  println(self)

  import context.dispatcher

  def receive = {
    case message: Notification =>
      val content = s"""{"color":"green","message":"${message.text}","notify":false,"message_format":"text"}"""

      wsClient.url(urlPrefix + s"room/$roomId/notification?auth_token=$token")
        .withHeaders("Content-Type" -> "application/json")
        .post(content)
        .onFailure {
          case e => println(s"Unable to send notification to Hipchat room: [${e.toString}")
        }
  }
}

object HipchatLogger {
  val urlPrefix = "https://api.hipchat.com/v2/"

  def props(parameters: HipchatConfigParameters_1_0_0): Props =
    Props(new HipchatLogger(parameters.token, parameters.roomId))
}