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
package observers

// scala
import scala.util.control.NonFatal

// awscala
import awscala.sqs.SQS

// eventhubs
import com.microsoft.azure.eventhubs.EventData
import com.microsoft.azure.eventprocessorhost.PartitionContext

// akka
import akka.actor.{Actor, ActorRef}

// java
import java.io.{ByteArrayInputStream, InputStream}
import java.nio.ByteBuffer
import java.nio.file._

// sauna
import loggers.Logger.Notification
import responders.Responder.S3Source

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
   * Common trait for observer events.
   */
  trait ObserverEvent extends Product with Serializable {
    /**
     * A full string representation of the event.
     */
    def id: String

    /**
     * The observer that emitted this event.
     */
    def observer: ActorRef
  }

  /**
   * Common trait for file-published events. Its' identifier is the path
   * to the file (so responders can decided whether handle it or not)
   * and it contains a content stream (so responders can read it)
   */
  sealed trait ObserverFileEvent extends ObserverEvent {
    /**
     * A file's content stream - can be streamed from a local filesystem, S3 etc.
     * None if the file cannot be streamed.
     *
     * This should never be a part of an object and shouldn't affect equality checks.
     */
    def streamContent: Option[InputStream]
  }

  /**
   * Common trait for command-based events. Always contains a content stream.
   */
  sealed trait ObserverCommandEvent extends ObserverEvent {
    /**
     * A stream containing the contents of the command.
     */
    def streamContent: InputStream
  }

  /**
   * File has been published on local filesystem
   *
   * @param file full root of file
   */
  case class LocalFilePublished(file: Path, observer: ActorRef) extends ObserverFileEvent {
    def id: String = file.toAbsolutePath.toString

    def streamContent: Option[InputStream] = try {
      Some(Files.newInputStream(file))
    } catch {
      case NonFatal(e) => None
    }
  }

  /**
   * File has been published on AWS S3
   *
   * @param id       full path on S3 bucket
   * @param s3Source AWS S3 credentials to access bucket and object
   */
  case class S3FilePublished(id: String, s3Source: S3Source, observer: ActorRef) extends ObserverFileEvent {
    def streamContent: Option[InputStream] = s3Source.s3.get(s3Source.bucket, id).map(_.content)
  }

  /**
   * Record has been received from AWS Kinesis Stream
   *
   * @param streamName name of kinesis stream
   * @param seqNr      unique sequence number assigned to record
   * @param data       buffer containing data carried by record
   * @param observer   observer that witnessed the record receipt
   */
  case class KinesisRecordReceived(streamName: String, seqNr: String, data: ByteBuffer, observer: ActorRef) extends ObserverCommandEvent {
    val byteBuffer = data
    val byteArray = new Array[Byte](byteBuffer.remaining())
    byteBuffer.get(byteArray)

    def id: String = s"kinesis-$streamName-$seqNr"

    def streamContent: ByteArrayInputStream = new ByteArrayInputStream(byteArray)
  }

  /**
   * Record has been received from Azure EventHubs
   *
   * @param context    Partition context
   * @param data       Event data
   * @param observer   observer that witnessed the record receipt
   */
  case class EventHubRecordReceived(context: PartitionContext, data: EventData, observer: ActorRef) extends ObserverCommandEvent {
    val byteArray = data.getBytes()
    
    def id: String = s"eventhub-${context.getPartitionId()}-${byteArray}"

    def streamContent: ByteArrayInputStream = new ByteArrayInputStream(byteArray)
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
