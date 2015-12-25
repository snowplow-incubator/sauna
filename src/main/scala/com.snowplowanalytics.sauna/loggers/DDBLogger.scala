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

// scala
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

trait DDBLogger extends Logger {

  /**
   * Writes the message to DynamoDb table.
   *
   * @param uid Unique identifier for message.
   * @param name Message header (for example, Optimizely list name).
   * @param status HTTP code for operation result.
   * @param description What happened.
   * @param lastModified Last modification time, if exists. Else, when message was processed.
   */
  override def manifestation(uid: String, name: String, status: Int, description: String, lastModified: String): Unit = {
    implicit val ddb = Sauna.ddb
    val ddbTable = Sauna.ddbTable

    val _ = Future { // make non-blocking call
      ddbTable.put(uid, name, "status" -> status, "description" -> description, "lastModified" -> lastModified)
    }
  }
}