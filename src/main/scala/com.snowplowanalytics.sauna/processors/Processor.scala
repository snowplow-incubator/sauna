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
import java.io.{File, InputStream}
import java.net.URLDecoder
import java.nio.file.Files

// akka
import akka.actor.Actor
import akka.actor.Status.{Success, Failure}

// awscala
import awscala.s3.S3

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
        cleanup(message)

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

  /**
   * Deletes processed object.
   *
   * @param fileAppeared Describes necessary data about newly appeared file.
   */
  def cleanup(fileAppeared: FileAppeared): Unit = {
    try {
      fileAppeared.location match {
        case InLocal =>
          val file = new File(fileAppeared.filePath)
          val _ = Files.deleteIfExists(file.toPath)

        case InS3(s3: S3, _bucketName: String, _fileName: String) =>
          val bucketName = URLDecoder.decode(_bucketName, "UTF-8")
          val fileName = URLDecoder.decode(_fileName, "UTF-8")
          s3.deleteObject(bucketName, fileName)
      }
      sender() ! Success

    } catch { case e: Exception =>
      System.err.println(s"Unable to delete [${fileAppeared.filePath}], that is located in [${fileAppeared.location}].")
      sender() ! Failure(e)
    }
  }
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
   * @param location Tells where this this file exists. See `Processor.FileLocation` for possible choices.
   */
  case class FileAppeared(filePath: String, is: InputStream, location: FileLocation)

  /**
   * Represents possible locations for newly appeared file. May be useful, for example,
   * for cleanup -- one should know where file is to delete it.
   */
  sealed trait FileLocation

  /**
   * For case when file is local.
   *
   * Note that it does not take any params, because filePath already is
   * in `FileAppeared`, so it can be an object.
   */
  object InLocal extends FileLocation

  /**
   * For case when file is in AWS S3.
   *
   * @param s3 An authorized instance of S3.
   * @param bucketName Name of bucket.
   * @param fileName Name of file.
   */
  case class InS3(s3: S3, bucketName: String, fileName: String) extends FileLocation
}