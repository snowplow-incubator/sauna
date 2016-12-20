/*
 * Copyright (c) 2012-2016 Snowplow Analytics Ltd. All rights reserved.
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
package observers

// scala
import scala.util.control.NonFatal

// awscala
import awscala.sqs.SQS
import com.amazonaws.services.kinesis.model.Record

// akka
import akka.actor.{ Actor, ActorRef }

// java
import java.io.InputStream
import java.nio.file._
import java.nio.ByteBuffer
import java.io.ByteArrayInputStream

// sauna
import responders.Responder.S3Source
import loggers.Logger.Notification

/**
 * Observers are entities responsible for keeping eye on some source source of
 * events, like filesystem, AWS S3/SQS. Also it is on observer's
 * responsibility to manipulate and cleanup resources after event has been
 * processed.
 */
trait Observer { self: Actor =>
  /**
   * Send message to supervisor trait to forward it to loggers
   * This means observer should also be a child of supvervisor
   */
  def notify(message: String): Unit = {
    context.parent ! Notification(message)
  }
}

object Observer {

  /**
   * Common trait for file-published event, which can provide access
   * to file path (so responders can decided whether handle it or not)
   * and to content stream (so responders can read it)
   */
  sealed trait ObserverBatchEvent extends Product with Serializable {
    /**
     * Full string representation of published file
     */
    def path: String

    /**
     * File's content stream. It can be streamed from local FS or from S3
     * None if file cannot be streamed
     * This should never be a part of object and wouldn't affect equality check
     */
    def streamContent: Option[InputStream]

    /**
     * Observer emitted event
     */
    def observer: ActorRef
  }

  /**
   * File has been published on local filesystem
   *
   * @param file full root of file
   */
  case class LocalFilePublished(file: Path, observer: ActorRef) extends ObserverBatchEvent {
    def path = file.toAbsolutePath.toString
    def streamContent = try {
      Some(Files.newInputStream(file))
    } catch {
      case NonFatal(e) => None
    }
  }



  /**
   * Record has been received from AWS Kinesis Stream
   *
   * @param streamName name of kinesis stream
   * @param record kinesis record
   */
  case class KinesisRecordReceived(streamName: String, seqNr: String, data: ByteBuffer, observer: ActorRef) extends ObserverBatchEvent {
    val byteBuffer = data
    val byteArray = new Array[Byte](byteBuffer.remaining())
    byteBuffer.get(byteArray)

    def path = s"${streamName}-${seqNr}"
    def streamContent = Some(new ByteArrayInputStream(byteArray))
  }

  /**
   * File has been published on AWS S3
   *
   * @param path full path on S3 bucket
   * @param s3Source AWS S3 credentials to access bucket and object
   */
  case class S3FilePublished(path: String, s3Source: S3Source, observer: ActorRef) extends ObserverBatchEvent {
    def streamContent = s3Source.s3.get(s3Source.bucket, path).map(_.content)
  }

  /**
   * Messages signaling to observer that resources need to be cleaned-up
   */
  sealed trait DeleteFile extends Product with Serializable {
    private[observers] def run(): Unit
  }
  case class DeleteLocalFile(path: Path) extends DeleteFile {
    private[observers] def run(): Unit =
      Files.delete(path)
  }
  case class DeleteS3Object(path: String, s3Source: S3Source) extends DeleteFile {
    private[observers] def run(): Unit =
      s3Source.s3.deleteObject(s3Source.bucket.name, path)

    private[observers] def deleteMessage(sqs: SQS): Unit =
      sqs.delete(s3Source.message)
  }
}
