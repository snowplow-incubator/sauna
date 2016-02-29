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

// sauna
import config._
import loggers.Logger._
import utils._

abstract class HipchatLogger(loggersConfig: LoggersConfig) extends Logger {
  import HipchatLogger._

  /**
   * Makes notification to some HipChat room.
   */
  override def log(message: Notification): Unit = {
    import message._

    val roomId = loggersConfig.hipchatRoomId
    val token = loggersConfig.hipchatToken
    val content = s"""{"color":"green","message":"$text","notify":false,"message_format":"text"}"""

    val _ = wsClient.url(urlPrefix + s"room/$roomId/notification")
                    .withHeaders("Authorization" -> s"Bearer $token", "Content-Type" -> "application/json")
                    .post(content)
  }
}

object HipchatLogger {
  val urlPrefix = "https://api.hipchat.com/v2/"
}