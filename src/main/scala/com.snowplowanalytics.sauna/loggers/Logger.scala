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
package com.snowplowanalytics.sauna.loggers

trait Logger {

  /**
   * Makes (unstructured) notification to somebody.
   *
   * @param message Text of notification.
   */
  def notification(message: String): Unit

  /**
   * Makes (structured) notification to somebody.
   * Note that these params describe a schema.
   *
   * @param uid Unique identifier for message.
   * @param name Message header (for example, Optimizely list name).
   * @param status HTTP code for operation result.
   * @param description What happened.
   * @param lastModified Last modification date, if exists, else - operation date.
   */
  def manifestation(uid: String, name: String, status: Int, description: String, lastModified: String)
}