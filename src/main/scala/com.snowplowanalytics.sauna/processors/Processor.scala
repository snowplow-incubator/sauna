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
package processors

// java
import java.io.InputStream

// akka
import akka.actor.Actor
import akka.actor.Status.{Success, Failure}

/**
 * After new file appeared, Sauna should process it somehow.
 * That's what a Processor does.
 */
trait Processor extends Actor {
  import Processor._

  override def receive = {
    case message: FileAppeared =>
      if (shouldProcess(message)) { // only if this file was processed
        process(message)
        sender() ! Success // here Success means that message was successfully handled by actor, not the result

      } else { // avoid possible timeout when asking for Future
        sender() ! Failure(new Exception(s"A FileAppeared from ${sender()} is not a subject of current Processor [${this.toString}]."))
      }

    case message => // avoid possible timeout when asking for Future
      sender() ! Failure(new Exception(s"${sender()} just sent strange message $message."))
  }

  /**
   * Describes a pattern for files that should be processed.
   */
  val pathPattern: String

  /**
   * Is this fileAppeared a subject for processing for current processor?
   * Note that in current implementation regexp may get evaluated twice,
   * if Processor's implementation needs captured groups.
   *
   * @param fileAppeared Describes necessary data about newly appeared file.
   * @return true if file is, and false otherwise.
   *         This result could be used, for example, for cleanup. So, if the file appeared in S3, it should
   *         be deleted in other manner than if it appeared locally.
   *         Also note that one could not simply delete FileAppeared.filePath, because a collision may
   *         happen - for example, if file appeared in S3, but there is a local file with exactly same filePath.
   */
  def shouldProcess(fileAppeared: FileAppeared): Boolean =
    fileAppeared.filePath.matches(pathPattern)

  /**
   * How to process new file.
   *
   * @param fileAppeared Describes necessary data about newly appeared file.
   */
  def process(fileAppeared: FileAppeared): Unit
}

object Processor {
  /**
   * File can be outside of local fs, e.g. on AWS S3, and
   * some important parameters might be encoded as part of 'filePath',
   * therefore this method has both 'filePath: String' and 'InputStream'.
   *
   * @param filePath Full path of file.
   * @param is InputStream from it.
   *           *** This field is mutable! ***
   */
  case class FileAppeared(filePath: String, is: InputStream)
}